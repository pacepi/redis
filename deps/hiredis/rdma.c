/* ==========================================================================
 * rdma.c - support RDMA protocol for transport layer.
 * --------------------------------------------------------------------------
 * Copyright (C) 2021  zhenwei pi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the
 * following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
 * USE OR OTHER DEALINGS IN THE SOFTWARE.
 * ==========================================================================
 */

#include "fmacros.h"

#include "async.h"
#include "async_private.h"
#include "hiredis.h"
#include "rdma.h"
#include <errno.h>

#define UNUSED(x) (void)(x)

void __redisSetError(redisContext *c, int type, const char *str);

static int rdma_debug = 0;
#define TRACE() if(rdma_debug){printf("%s %d\n", __func__, __LINE__);}

#ifdef USE_RDMA
#ifdef __linux__    /* currently RDMA is supported only on Linux */
#include <arpa/inet.h>
#include <assert.h>
#include <limits.h>
#include <netdb.h>
#include <poll.h>
#include <rdma/rdma_cma.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>

/* send 'command' to server, server side replies lots of data segments */
#define REDIS_MAX_SGE 512

typedef struct RdmaContext {
    struct rdma_cm_id *cm_id;
    struct rdma_event_channel *cm_channel;
    struct ibv_pd *pd;
    int cm_state;

    int send_pending;
    struct ibv_comp_channel *send_channel;
    struct ibv_cq *send_cq;
    void *send_buf;
    struct ibv_send_wr send_wr;
    struct ibv_mr *send_mr;
    struct ibv_sge send_sge;

    struct ibv_comp_channel *recv_channel;
    struct ibv_cq *recv_cq;
    void *recv_buf;
    struct ibv_recv_wr recv_wrs[REDIS_MAX_SGE];
    struct ibv_mr *recv_mr;
    struct ibv_sge recv_sges[REDIS_MAX_SGE];
} RdmaContext;

int redisContextTimeoutMsec(redisContext *c, long *result);
int redisContextUpdateConnectTimeout(redisContext *c, const struct timeval *timeout);
int redisSetFdBlocking(redisContext *c, redisFD fd, int blocking);

static inline long redisNowMs(void) {
    struct timeval tv;

    if (gettimeofday(&tv, NULL) < 0)
            return -1;

    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static void rdmaDestroyIoBuf(RdmaContext *ctx)
{
TRACE();
    if (ctx->recv_mr) {
        ibv_dereg_mr(ctx->recv_mr);
        hi_free(ctx->recv_buf);
        ctx->recv_buf = NULL;
        ctx->recv_mr = NULL;
    }

    if (ctx->send_mr) {
        ibv_dereg_mr(ctx->send_mr);
        hi_free(ctx->send_buf);
        ctx->send_buf = NULL;
        ctx->send_mr = NULL;
    }
}

static int rdmaSetupIoBuf(redisContext *c, RdmaContext *ctx, struct rdma_cm_id *cm_id)
{
TRACE();
    int access = IBV_ACCESS_LOCAL_WRITE, i, ret;
    size_t length = REDIS_READER_MAX_BUF;
    struct ibv_sge *sge;
    struct ibv_recv_wr *bad_wr;
    struct ibv_recv_wr *recv_wr;

    /* setup recv buf & MR */
    ctx->recv_buf = hi_calloc(length, REDIS_MAX_SGE);
    ctx->recv_mr = ibv_reg_mr(ctx->pd, ctx->recv_buf, length * REDIS_MAX_SGE, access);
    if (!ctx->recv_mr) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: reg recv mr failed");
        hi_free(ctx->recv_buf);
        ctx->recv_buf = NULL;
        return REDIS_ERR;
    }

    for (i = 0; i < REDIS_MAX_SGE; i++) {
        sge = &ctx->recv_sges[i];
        sge->addr = (uint64_t)ctx->recv_buf + i * length;
        sge->length = length;
        sge->lkey = ctx->recv_mr->lkey;

        recv_wr = &ctx->recv_wrs[i];
        recv_wr->wr_id = (uint64_t)recv_wr;
        recv_wr->sg_list = sge;
        recv_wr->num_sge = 1;
        recv_wr->next = NULL;

        ret = ibv_post_recv(cm_id->qp, recv_wr, &bad_wr);
        if (ret < 0) {
            __redisSetError(c, REDIS_ERR_OTHER, "RDMA: post recv failed");
            goto destroy_iobuf;
       }
    }

    /* setup send buf & MR */
    length = REDIS_READER_MAX_BUF;
    ctx->send_buf = hi_calloc(length, 1);
    ctx->send_mr = ibv_reg_mr(ctx->pd, ctx->send_buf, length, access);
    if (!ctx->send_mr) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: reg send mr failed");
        hi_free(ctx->send_buf);
        ctx->send_buf = NULL;
        goto destroy_iobuf;
    }

    sge = &ctx->send_sge;
    sge->addr = (uint64_t)ctx->send_buf;
    sge->length = length;
    sge->lkey = ctx->send_mr->lkey;
    ctx->send_wr.opcode = IBV_WR_SEND;
    ctx->send_wr.send_flags = 0;
    ctx->send_wr.sg_list = sge;
    ctx->send_wr.num_sge = 1;

    return REDIS_OK;

destroy_iobuf:
    rdmaDestroyIoBuf(ctx);
    return REDIS_ERR;
}

static void redisRdmaClose(redisContext *c) {
    RdmaContext *ctx = (RdmaContext *)c->privctx;
    struct rdma_cm_id *cm_id = ctx->cm_id;

    TRACE();
    rdmaDestroyIoBuf(ctx);
    ibv_destroy_qp(cm_id->qp);
    ibv_destroy_cq(ctx->recv_cq);
    ibv_destroy_cq(ctx->send_cq);
    ibv_destroy_comp_channel(ctx->send_channel);
    ibv_destroy_comp_channel(ctx->recv_channel);
    ibv_dealloc_pd(ctx->pd);
    rdma_destroy_id(cm_id);

    rdma_destroy_event_channel(ctx->cm_channel);
}

static void redisRdmaFree(void *privctx) {
    TRACE();
    if (!privctx)
        return;

    hi_free(privctx);
}

static int connRdmaHandleCq(redisContext *c, int is_write, void *buf, size_t buf_len) {
    RdmaContext *ctx = (RdmaContext *)c->privctx;
    struct rdma_cm_id *cm_id = ctx->cm_id;
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;
    struct ibv_wc wc = {0};
    struct ibv_recv_wr *recv_wr, *bad_wr;
    struct ibv_comp_channel *channel;
    int ret;
    long idx;
TRACE();

    UNUSED(buf_len);    //TODO to implement

    channel = is_write ? ctx->send_channel : ctx->recv_channel;
    if (ibv_get_cq_event(channel, &ev_cq, &ev_ctx) < 0) {
        if (errno != EAGAIN) {
TRACE();
            __redisSetError(c, REDIS_ERR_OTHER, "RDMA: get cq event failed");
            return REDIS_ERR;
        }
TRACE();
    } else if (ibv_req_notify_cq(ev_cq, 0)) {
TRACE();
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: notify cq failed");
        return REDIS_ERR;
    }

 if (rdma_debug) printf("handleCQ: %s\n", ctx->recv_cq == ev_cq ? "RECV" : ctx->send_cq == ev_cq ? "SEND" : "UNKNOWN");

    /* even no new CQ event, still try to poll cq. because to query a single wc
     * every time, it works like event triggered. */
    if (!ev_cq) {
        ev_cq = is_write ? ctx->send_cq : ctx->recv_cq;
    }

    ret = ibv_poll_cq(ev_cq, 1, &wc);
    if (ret < 0) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: poll cq failed");
        return REDIS_ERR;
    } else if (ret == 0) {
        return 0;
    }

    ibv_ack_cq_events(ev_cq, 1);

    if (rdma_debug) printf("status 0x%x, opcode 0x%x, byte_len %d\n", wc.status, wc.opcode, wc.byte_len);

    if (wc.status != IBV_WC_SUCCESS) {
        printf("RDMA: CQ handle error status 0x%x\n", wc.status);
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: send/recv failed");
        return REDIS_ERR;
    }

    switch (wc.opcode) {
    case IBV_WC_RECV:
        idx = ((char *)wc.wr_id - (char *)ctx->recv_wrs) / sizeof(struct ibv_recv_wr);
        memcpy(buf, (char *)ctx->recv_buf + idx * REDIS_READER_MAX_BUF, wc.byte_len);

        recv_wr = (struct ibv_recv_wr *)wc.wr_id;
        ret = ibv_post_recv(cm_id->qp, recv_wr, &bad_wr);
        if (ret < 0) {
             printf("RDMA: post recv failed: ret %d, %d, %s, bad_wr %p(offset %ld)\n", ret, errno, strerror(errno), (void *)bad_wr, (char *)bad_wr - (char *)ctx->recv_wrs);
            return REDIS_ERR;
        }

        ret = wc.byte_len;

        if (rdma_debug) printf("RDMA: RECV [%d]\n", ret);
        //((char *)buf)[ret + 1] = 0;
        //if (rdma_debug) printf("RDMA RECV [%d] %s\n", ret, (char *)buf);
        break;

    case IBV_WC_SEND:
        ctx->send_pending = 0;
        /* wc.byte_len makes sense. RXE implements this, but Mellanox CX5 doesn't */
        ret = buf_len;

        if (rdma_debug) printf("RDMA: SEND [%d]\n", ret);
        //((char *)buf)[ret + 1] = 0;
        //if (rdma_debug) printf("RDMA SEND [%d] %s\n", ret, (char *)buf);
        break;

    default:
        if (rdma_debug) printf("RDMA: unsupported wc opcode[%d]\n", wc.opcode);
        return REDIS_ERR;
    }

    return ret;
}

#define __MAX_MSEC (((LONG_MAX) - 999) / 1000)

static int redisCommandTimeoutMsec(redisContext *c, long *result)
{
    const struct timeval *timeout = c->command_timeout;
    long msec = -1;

    /* Only use timeout when not NULL. */
    if (timeout != NULL) {
        if (timeout->tv_usec > 1000000 || timeout->tv_sec > __MAX_MSEC) {
            *result = msec;
            return REDIS_ERR;
        }

        msec = (timeout->tv_sec * 1000) + ((timeout->tv_usec + 999) / 1000);

        if (msec < 0 || msec > INT_MAX) {
            msec = INT_MAX;
        }
    }

    *result = msec;
    return REDIS_OK;
}

static ssize_t redisRdmaRead(redisContext *c, char *buf, size_t bufcap) {
    RdmaContext *ctx = (RdmaContext *)c->privctx;
    struct pollfd pfd;
    int block = c->flags & REDIS_BLOCK;
    long timed = -1, elapsed = 0;
    int ret;

TRACE();

    if (redisCommandTimeoutMsec(c, &timed)) {
        return REDIS_ERR;
    }

retry:
    /* try to poll a CQ firstly */
    ret = connRdmaHandleCq(c, 0, buf, bufcap);
    if (ret) {
        return ret;
    }

    if (block) {
        pfd.fd = ctx->recv_channel->fd;
        pfd.events = POLLIN;
        pfd.revents = 0;
        if (poll(&pfd, 1, 1) < 0) {
            return REDIS_ERR;
        }

    if ((timed > 0) && (++elapsed < timed)) {
            goto retry;
        }
    }

    return 0;
}

static ssize_t redisRdmaWrite(redisContext *c) {
    RdmaContext *ctx = (RdmaContext *)c->privctx;
    struct rdma_cm_id *cm_id = ctx->cm_id;
    struct ibv_send_wr *bad_wr;
    size_t len = hi_sdslen(c->obuf);
    struct pollfd pfd;
    int block = c->flags & REDIS_BLOCK;
    long timed = -1;
    int ret;

TRACE();
    memcpy(ctx->send_buf, c->obuf, len);
    ctx->send_sge.length = len;
    ctx->send_wr.opcode = IBV_WR_SEND;
    ctx->send_wr.send_flags = IBV_SEND_SIGNALED;
    ctx->send_wr.next = NULL;
    ret = ibv_post_send(cm_id->qp, &ctx->send_wr, &bad_wr);
    if (ret < 0) {
        __redisSetError(c, REDIS_ERR_IO, "RDMA: post send failed");
        return REDIS_ERR;
    }

    ctx->send_pending = len;

    if (redisCommandTimeoutMsec(c, &timed)) {
        return REDIS_ERR;
    }

    if (block) {
TRACE();
        pfd.fd = ctx->send_channel->fd;
        pfd.events = POLLIN;
        pfd.revents = 0;
        if (poll(&pfd, 1, timed) < 0) {
            return REDIS_ERR;
        }
    }

TRACE();
    ret = connRdmaHandleCq(c, 1, c->obuf, len);

    return ret;
}

/* RDMA has no POLLOUT event supported, so it could't work well with hiredis async mechanism */
void redisRdmaAsyncRead(redisAsyncContext *ac) {
    UNUSED(ac);
    assert("hiredis async mechanism can't work with RDMA" == NULL);
}

void redisRdmaAsyncWrite(redisAsyncContext *ac) {
    UNUSED(ac);
    assert("hiredis async mechanism can't work with RDMA" == NULL);
}

redisContextFuncs redisContextRdmaFuncs = {
    .close = redisRdmaClose,
    .free_privctx = redisRdmaFree,
    .async_read = redisRdmaAsyncRead,
    .async_write = redisRdmaAsyncWrite,
    .read = redisRdmaRead,
    .write = redisRdmaWrite,
};


static int redisRdmaConnect(redisContext *c, struct rdma_cm_id *cm_id) {
    RdmaContext *ctx = (RdmaContext *)c->privctx;
    struct ibv_comp_channel *send_channel = NULL;
    struct ibv_comp_channel *recv_channel = NULL;
    struct ibv_cq *send_cq = NULL;
    struct ibv_cq *recv_cq = NULL;
    struct ibv_pd *pd = NULL;
    struct ibv_qp_init_attr init_attr = {0};
    struct rdma_conn_param conn_param = {0};

    pd = ibv_alloc_pd(cm_id->verbs);
    if (!pd) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: alloc pd failed");
        goto error;
    }

    /* setup send channel/cq */
    send_channel = ibv_create_comp_channel(cm_id->verbs);
    if (!send_channel) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: alloc pd failed");
        goto error;
    }

    if (redisSetFdBlocking(c, send_channel->fd, 0) != REDIS_OK) {
TRACE();
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: set recv comp channel fd non-block failed");
        goto error;
    }

    send_cq = ibv_create_cq(cm_id->verbs, 1, ctx, send_channel, 0);
    if (!send_cq) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: create send cq failed");
        goto error;
    }

    if (ibv_req_notify_cq(send_cq, 0)) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: notify send cq failed");
        goto error;
    }

    /* setup recv channel/cq */
    recv_channel = ibv_create_comp_channel(cm_id->verbs);
    if (!recv_channel) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: alloc pd failed");
        goto error;
    }

    if (redisSetFdBlocking(c, recv_channel->fd, 0) != REDIS_OK) {
TRACE();
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: set recv comp channel fd non-block failed");
        goto error;
    }

    recv_cq = ibv_create_cq(cm_id->verbs, REDIS_MAX_SGE, ctx, recv_channel, 0);
    if (!recv_cq) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: create recv cq failed");
        goto error;
    }

    if (ibv_req_notify_cq(recv_cq, 0)) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: notify recv cq failed");
        goto error;
    }

    /* create qp with attr */
    init_attr.cap.max_send_wr = 1;
    init_attr.cap.max_recv_wr = REDIS_MAX_SGE; 
    init_attr.cap.max_send_sge = 1; 
    init_attr.cap.max_recv_sge = 1; 
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = send_cq;
    init_attr.recv_cq = recv_cq;
    if (rdma_create_qp(cm_id, pd, &init_attr)) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: create qp failed");
        goto error;
    }

    ctx->cm_id = cm_id;
    ctx->send_channel = send_channel;
    ctx->recv_channel = recv_channel;
    ctx->send_cq = send_cq;
    ctx->recv_cq = recv_cq;
    ctx->pd = pd;

    if (rdmaSetupIoBuf(c, ctx, cm_id) != REDIS_OK)
        goto free_qp;

    /* rdma connect with param */
    conn_param.responder_resources = 1;
    conn_param.initiator_depth = 1;
    conn_param.retry_count = 7;
    conn_param.rnr_retry_count = 7;
    if (rdma_connect(cm_id, &conn_param)) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: connect failed");
        goto destroy_iobuf;
    }

    return REDIS_OK;

destroy_iobuf:
    rdmaDestroyIoBuf(ctx);
free_qp:
    ibv_destroy_qp(cm_id->qp);
error:
    if (send_cq)
        ibv_destroy_cq(send_cq);
    if (recv_cq)
        ibv_destroy_cq(recv_cq);
    if (pd)
        ibv_dealloc_pd(pd);
    if (send_channel)
        ibv_destroy_comp_channel(send_channel);
    if (recv_channel)
        ibv_destroy_comp_channel(recv_channel);

    return REDIS_ERR;
}

static int redisRdmaCM(redisContext *c, int timeout) {
    RdmaContext *ctx = (RdmaContext *)c->privctx;
    struct rdma_cm_event *event;
    char errorstr[128];
    int ret = REDIS_ERR;

     if (rdma_debug) printf("RDMA: handle cm channel %p\n", (void *)ctx->cm_channel);
    while (rdma_get_cm_event(ctx->cm_channel, &event) == 0) {
     if (rdma_debug) printf("RDMA: handle cm event id %p, listen id %p, event %d(%s), status %d\n", (void *)event->id, (void *)event->listen_id, event->event, rdma_event_str(event->event), event->status);
        switch (event->event) {
        case RDMA_CM_EVENT_ADDR_RESOLVED:
            if (timeout < 0 || timeout > 100)
                timeout = 100; /* at most 100ms to resolve route */
            ret = rdma_resolve_route(event->id, timeout);
            if (ret) {
                __redisSetError(c, REDIS_ERR_OTHER, "RDMA: route resolve failed");
            }
            break;
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
            ret = redisRdmaConnect(c, event->id);
            break;
        case RDMA_CM_EVENT_ESTABLISHED:
            /* it's time to tell redis we have already connected */
            c->flags |= REDIS_CONNECTED;
            c->funcs = &redisContextRdmaFuncs;
            c->fd = ctx->recv_channel->fd;
            ret = REDIS_OK;
            if (rdma_debug) printf("RDMA: redisContext fd %d\n", c->fd);
            break;
        case RDMA_CM_EVENT_TIMEWAIT_EXIT:
            ret = REDIS_ERR;
            __redisSetError(c, REDIS_ERR_TIMEOUT, "RDMA: connect timeout");
            break;
        case RDMA_CM_EVENT_ADDR_ERROR:
        case RDMA_CM_EVENT_ROUTE_ERROR:
        case RDMA_CM_EVENT_CONNECT_ERROR:
        case RDMA_CM_EVENT_UNREACHABLE:
        case RDMA_CM_EVENT_REJECTED:
        case RDMA_CM_EVENT_DISCONNECTED:
        case RDMA_CM_EVENT_ADDR_CHANGE:
        default:
            snprintf(errorstr, sizeof(errorstr), "RDMA: connect failed - %s", rdma_event_str(event->event));
            __redisSetError(c, REDIS_ERR_OTHER, errorstr);
            ret = REDIS_ERR;
            break;
        }

        rdma_ack_cm_event(event);
	ctx->cm_state = event->event;
    }

    return ret;
}

static int redisRdmaWaitConn(redisContext *c, long timeout) {
    int timed;
    struct pollfd pfd;
    long now = redisNowMs();
    long start = now;
    RdmaContext *ctx = (RdmaContext *)c->privctx;

TRACE();
    while (now - start < timeout) {
        timed = (int)(timeout - (now - start));

        pfd.fd = ctx->cm_channel->fd;
        pfd.events = POLLIN;
        pfd.revents = 0;
        if (poll(&pfd, 1, timed) < 0) {
            return REDIS_ERR;
        }

        if (redisRdmaCM(c, timed) == REDIS_ERR) {
            return REDIS_ERR;
        }

        if (c->flags & REDIS_CONNECTED) {
            return REDIS_OK;
        }

        now = redisNowMs();
    }

    return REDIS_ERR;
}

int redisContextConnectRdma(redisContext *c, const char *addr, int port,
                            const struct timeval *timeout) {
    int ret;
    char _port[6];  /* strlen("65535"); */
    struct addrinfo hints, *servinfo, *p;
    long timeout_msec = -1;
    struct rdma_event_channel *cm_channel = NULL;
    struct rdma_cm_id *cm_id = NULL;
    RdmaContext *ctx = NULL;
    struct sockaddr_storage saddr;
    long start = redisNowMs(), timed;

    servinfo = NULL;
    c->connection_type = REDIS_CONN_RDMA;
    c->tcp.port = port;

    if (c->tcp.host != addr) {
        hi_free(c->tcp.host);

        c->tcp.host = hi_strdup(addr);
        if (c->tcp.host == NULL) {
            __redisSetError(c, REDIS_ERR_OOM, "RDMA: Out of memory");
            return REDIS_ERR;
        }
    }

    if (timeout) {
        if (redisContextUpdateConnectTimeout(c, timeout) == REDIS_ERR) {
            __redisSetError(c, REDIS_ERR_OOM, "RDMA: Out of memory");
            return REDIS_ERR;
        }
    } else {
        hi_free(c->connect_timeout);
        c->connect_timeout = NULL;
    }

    if (redisContextTimeoutMsec(c, &timeout_msec) != REDIS_OK) {
        __redisSetError(c, REDIS_ERR_IO, "RDMA: Invalid timeout specified");
        return REDIS_ERR;
    } else if (timeout_msec == -1) {
        timeout_msec = INT_MAX;
    }

    snprintf(_port, 6, "%d", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if ((ret = getaddrinfo(c->tcp.host, _port, &hints, &servinfo)) != 0) {
         hints.ai_family = AF_INET6;
         if ((ret = getaddrinfo(addr, _port, &hints, &servinfo)) != 0) {
            __redisSetError(c, REDIS_ERR_OTHER, gai_strerror(ret));
            return REDIS_ERR;
        }
    }

    ctx = hi_calloc(sizeof(RdmaContext), 1);
    if (!ctx) {
        __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
        goto error;
    }

    c->privctx = ctx;

   cm_channel = rdma_create_event_channel();
    if (!cm_channel) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: create event channel failed");
        goto error;
    }
     if (rdma_debug) printf("RDMA: create cm channel %p\n", (void *)cm_channel);

    ctx->cm_channel = cm_channel;

    if (rdma_create_id(cm_channel, &cm_id, (void *)ctx, RDMA_PS_TCP)) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: create id failed");
        return REDIS_ERR;
    }
     if (rdma_debug) printf("RDMA: create cm id %p\n", (void *)cm_id);
    ctx->cm_id = cm_id;

    if ((redisSetFdBlocking(c, cm_channel->fd, 0) != REDIS_OK)) {
TRACE();
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: set cm channel fd non-block failed");
        goto free_rdma;
    }

    for (p = servinfo; p != NULL; p = p->ai_next) {
TRACE();
        if (p->ai_family == PF_INET) {
                memcpy(&saddr, p->ai_addr, sizeof(struct sockaddr_in));
                ((struct sockaddr_in *)&saddr)->sin_port = htons(port);
        } else if (p->ai_family == PF_INET6) {
                memcpy(&saddr, p->ai_addr, sizeof(struct sockaddr_in6));
                ((struct sockaddr_in6 *)&saddr)->sin6_port = htons(port);
        } else {
            __redisSetError(c, REDIS_ERR_PROTOCOL, "RDMA: unsupported family");
            goto free_rdma;
        }

        /* resolve addr as most 100ms */
        if (rdma_resolve_addr(cm_id, NULL, (struct sockaddr *)&saddr, 100)) {
            continue;
        }

        timed = timeout_msec - (redisNowMs() - start);
        if ((redisRdmaWaitConn(c, timed) == REDIS_OK) && (c->flags & REDIS_CONNECTED)) {
     if (rdma_debug) printf("RDMA: connection flags 0x%x\n", c->flags);
            ret = REDIS_OK;
            goto end;
        }
    }

    if ((!c->err) && (p == NULL)) {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: resolve failed");
    }

free_rdma:
    if (cm_id) {
        rdma_destroy_id(cm_id);
    }
    if (cm_channel) {
        rdma_destroy_event_channel(cm_channel);
    }

error:
    ret = REDIS_ERR;
    if (ctx) {
        hi_free(ctx);
    }

end:
    if(servinfo) {
        freeaddrinfo(servinfo);
    }

    return ret;
}

#else    /* __linux__ */

"BUILD ERROR: RDMA is supported only on linux"

#endif   /* __linux__ */
#else    /* USE_RDMA */

int redisContextConnectRdma(redisContext *c, const char *addr, int port,
                            const struct timeval *timeout) {
    UNUSED(c);
    UNUSED(addr);
    UNUSED(port);
    UNUSED(timeout);
    __redisSetError(c, REDIS_ERR_PROTOCOL, "RDMA: disabled, please rebuild with BUILD_RDMA");
    return -EPROTONOSUPPORT;
}

#endif
