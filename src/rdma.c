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

#include "server.h"
#include "connection.h"
#include "connhelpers.h"

static void serverNetError(char *err, const char *fmt, ...) {
    va_list ap; 

    if (!err) return;
    va_start(ap, fmt);
    vsnprintf(err, ANET_ERR_LEN, fmt, ap);
    va_end(ap);
}

static int debug_rmda = 0;
#define TRACE() if (debug_rmda){serverLog(LL_WARNING, "%s %d\n", __func__, __LINE__);}

#ifdef USE_RDMA
#ifdef __linux__    /* currently RDMA is supported only on Linux */
#include <arpa/inet.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <sys/types.h>
#include <sys/socket.h>

#define WRITE_CQ_POLL_MAX 10000

typedef struct rdma_connection {
    connection c;
    struct rdma_cm_id *cm_id;
    int last_errno;
} rdma_connection;

typedef struct RdmaContext {
    connection *conn;
    char *ip;
    int port;
    bool cm_disconnected;
    struct ibv_pd *pd;

    int send_handler;
    int send_pending;
    void *send_buf;
    struct ibv_comp_channel *send_channel;
    struct ibv_cq *send_cq;
    struct ibv_send_wr send_wr;
    struct ibv_mr *send_mr;
    struct ibv_sge send_sge;

    void *recv_buf;
    struct ibv_comp_channel *recv_channel;
    struct ibv_cq *recv_cq;
    struct ibv_recv_wr recv_wr;
    struct ibv_mr *recv_mr;
    struct ibv_sge recv_sge;
} RdmaContext;

static struct rdma_event_channel *listen_channel;
static struct rdma_cm_id *listen_cmids[CONFIG_BINDADDR_MAX];

static void rdmaDestroyIoBuf(RdmaContext *ctx)
{
TRACE();
    if (ctx->recv_mr) {
        ibv_dereg_mr(ctx->recv_mr);
        zfree(ctx->recv_buf);
        ctx->recv_buf = NULL;
        ctx->recv_mr = NULL;
    }

    if (ctx->send_mr) {
        ibv_dereg_mr(ctx->send_mr);
        zfree(ctx->send_buf);
        ctx->send_buf = NULL;
        ctx->send_mr = NULL;
    }
}

static int rdmaSetupIoBuf(char *err, RdmaContext *ctx, struct rdma_cm_id *cm_id)
{
TRACE();
    int access = IBV_ACCESS_LOCAL_WRITE;
    size_t length = PROTO_INLINE_MAX_SIZE;
    struct ibv_sge *sge;
    struct ibv_recv_wr *bad_wr;

    /* setup recv buf & MR */
    ctx->recv_buf = zcalloc(length);
    ctx->recv_mr = ibv_reg_mr(ctx->pd, ctx->recv_buf, length, access);
    if (!ctx->recv_mr) {
        serverNetError(err, "RDMA: reg recv mr failed");
        zfree(ctx->recv_buf);
        ctx->recv_buf = NULL;
	return C_ERR;
    }

    sge = &ctx->recv_sge;
    sge->addr = (uint64_t)ctx->recv_buf;
    sge->length = length;
    sge->lkey = ctx->recv_mr->lkey;
    ctx->recv_wr.sg_list = sge;
    ctx->recv_wr.num_sge = 1;
    ctx->recv_wr.next = NULL;

    if (ibv_post_recv(cm_id->qp, &ctx->recv_wr, &bad_wr) < 0) {
        serverNetError(err, "RDMA: post recv failed");
        goto destroy_iobuf;
    }

    /* setup send buf & MR */
    length = PROTO_IOBUF_LEN;
    ctx->send_buf = zcalloc(length);
    ctx->send_mr = ibv_reg_mr(ctx->pd, ctx->send_buf, length, access);
    if (!ctx->send_mr) {
        serverNetError(err, "RDMA: reg send mr failed");
        zfree(ctx->send_buf);
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

    return C_OK;

destroy_iobuf:
    rdmaDestroyIoBuf(ctx);
    return C_ERR;
}

static int connRdmaAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
TRACE();
    int ret = C_OK;

    if (conn->state != CONN_STATE_ACCEPTING)
        return C_ERR;

    conn->state = CONN_STATE_CONNECTED;

    connIncrRefs(conn);
    if (!callHandler(conn, accept_handler))
        ret = C_ERR;
    connDecrRefs(conn);

    return ret;
}

static int connRdmaSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
TRACE();
    UNUSED(conn);
    UNUSED(func);

    if (barrier)
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    else
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;

    return C_OK;
}

static int connRdmaSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;

TRACE();
    /* save conn into RdmaContext */
    ctx->conn = conn;

    conn->read_handler = func;
    if (!conn->read_handler)
        aeDeleteFileEvent(server.el, conn->fd, AE_READABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,
                    AE_READABLE,conn->type->ae_handler,conn) == AE_ERR)
            return C_ERR;

    return C_OK;
}

static void connRdmaEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask) {
TRACE();
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);
    connection *conn = clientData;

if (debug_rmda) serverLog(LL_WARNING, "%s: state %d\n", __func__, conn->state);//TODO remove

    if (conn->read_handler) {
        callHandler(conn, conn->read_handler);
    }
}

static const char *connRdmaGetLastError(connection *conn) {
TRACE();
    return strerror(conn->last_errno);
}

static int connRdmaConnect(connection *conn_, const char *addr, int port, const char *src_addr, ConnectionCallbackFunc connect_handler) {
    UNUSED(conn_);
    UNUSED(addr);
    UNUSED(port);
    UNUSED(src_addr);
    UNUSED(connect_handler);

    serverLog(LL_WARNING, "RDMA: client side is not supported now");
    return C_ERR;
}

static int connRdmaBlockingConnect(connection *conn, const char *addr, int port, long long timeout) {
    UNUSED(conn);
    UNUSED(addr);
    UNUSED(port);
    UNUSED(timeout);

    serverLog(LL_WARNING, "RDMA: client side is not supported now");
    return C_ERR;
}

static void connRdmaClose(connection *conn) {
TRACE();
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;

    if (conn->fd != -1) {
        aeDeleteFileEvent(server.el,conn->fd, AE_READABLE | AE_WRITABLE);
        rdma_disconnect(cm_id);
        conn->fd = -1;
    }

    aeDeleteFileEvent(server.el, ctx->send_channel->fd, AE_READABLE);

    rdmaDestroyIoBuf(ctx);
    ibv_destroy_qp(cm_id->qp);
    ibv_destroy_cq(ctx->send_cq);
    ibv_destroy_cq(ctx->recv_cq);
    ibv_destroy_comp_channel(ctx->send_channel);
    ibv_destroy_comp_channel(ctx->recv_channel);
    ibv_dealloc_pd(ctx->pd);
    rdma_destroy_id(cm_id);

    zfree(conn);
TRACE();
}

static int connRdmaHandleCq(rdma_connection *conn, int is_write, void *buf, size_t buf_len) {
    int ret = C_ERR;
    struct rdma_cm_id *cm_id = conn->cm_id;
    RdmaContext *ctx = cm_id->context;
    struct ibv_cq *ev_cq = NULL;
    struct ibv_comp_channel *channel = NULL;
    void *ev_ctx = NULL;
    struct ibv_wc wc = {0};
    struct ibv_recv_wr *bad_wr = NULL;

    if (unlikely(ctx->cm_disconnected && !is_write))
        return 0;  /* let .read method to cancel connection */

    UNUSED(buf_len);	//TODO to implement

    channel = is_write ? ctx->send_channel : ctx->recv_channel;
    if (ibv_get_cq_event(channel, &ev_cq, &ev_ctx) < 0) {
        if (errno != EAGAIN) {
            serverLog(LL_WARNING, "RDMA: get CQ event error");
            return C_ERR;
        }
        return 0;
    } else if (ibv_req_notify_cq(ev_cq, 0)) {
        serverLog(LL_WARNING, "RDMA: notify CQ error");
        return C_ERR;
    }

    if (debug_rmda) serverLog(LL_WARNING, "RDMA: handleCQ: %s\n", ctx->recv_cq == ev_cq ? "RECV" : ctx->send_cq == ev_cq ? "SEND" : "UNKNOWN");//TODO remove

    ret = ibv_poll_cq(ev_cq, 1, &wc);
    if (ret < 0) {
        serverLog(LL_WARNING, "RDMA: poll recv CQ error");
        return C_ERR;
    } else if (ret == 0) {
        return 0;
    }

    ibv_ack_cq_events(ev_cq, 1);

    if (debug_rmda) serverLog(LL_WARNING, "status 0x%x, opcode 0x%x, byte_len %d\n", wc.status, wc.opcode, wc.byte_len);

    if (wc.status != IBV_WC_SUCCESS) {
        serverLog(LL_WARNING, "RDMA: CQ handle error status 0x%x\n", wc.status);
        ctx->cm_disconnected = true;
        return C_ERR;
    }

    switch (wc.opcode) {
    case IBV_WC_RECV:
        if (ev_cq != ctx->recv_cq) {
           serverLog(LL_WARNING, "RDMA: FATAL error, recv event happened on unexpected CQ\n");
           return C_ERR;
        }

        memcpy(buf, ctx->recv_buf, wc.byte_len);
        ret = wc.byte_len;

        if (ibv_post_recv(cm_id->qp, &ctx->recv_wr, &bad_wr) < 0) {
            serverLog(LL_WARNING, "RDMA: post recv failed\n");
            return C_ERR;
        }

        if (debug_rmda) serverLog(LL_WARNING, "RDMA: RECV [%d]\n", ret);
	//((char *)ctx->recv_buf)[ret] = 0;	//TODO remove
        //if (debug_rmda) serverLog(LL_WARNING, "RDMA RECV [%d] %s", ret, (char *)ctx->recv_buf);
        break;

    case IBV_WC_SEND:
        if (ev_cq != ctx->send_cq) {
           serverLog(LL_WARNING, "RDMA: FATAL error, send event happened on unexpected CQ\n");
           return C_ERR;
        }

if (!ctx->send_pending)
    printf("FATAL error, ctx->send_pending 0\n");
        ctx->send_pending = 0;
        /* returning wc.byte_len makes sense. RXE implements this, but CX5 doesn't */
        ret = buf_len;

        if (debug_rmda) serverLog(LL_WARNING, "RDMA: SEND [%d]", ret);
	//((char *)ctx->send_buf)[ret] = 0;	//TODO remove
        //if (debug_rmda) serverLog(LL_WARNING, "RDMA SEND [%d] %s", ret, (char *)ctx->send_buf);
        break;

    default:
        serverLog(LL_WARNING, "RDMA: unexpected opcode 0x[%x]\n", wc.opcode);
        return C_ERR;
    }

    return ret;
}

static void connRdmaSendChannelHandler(struct aeEventLoop *el, int fd, void *clientData, int mask) {
    rdma_connection *rdma_conn = (rdma_connection *)clientData;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;

    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    TRACE();

    connRdmaHandleCq(rdma_conn, 1, NULL, ctx->send_pending);
}

static int connRdmaWrite(connection *conn, const void *data, size_t data_len) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;
    struct ibv_send_wr *bad_wr;
    int ret = C_OK, pollcq = WRITE_CQ_POLL_MAX;

    if (!ctx->send_handler) {
        if (aeCreateFileEvent(server.el, ctx->send_channel->fd, AE_READABLE, connRdmaSendChannelHandler, conn) == AE_ERR) {
            return C_ERR;
        }
	ctx->send_handler = 1;
    }

    while (ctx->send_pending) {
        ret = connRdmaHandleCq(rdma_conn, 1, NULL, ctx->send_pending);
	if (ret == C_ERR) {
            return C_ERR;
        }
	if (--pollcq < 0) {
            serverLog(LL_WARNING, "RDMA: write timeout\n");
            return C_ERR;
        }
    }

    memcpy(ctx->send_buf, data, data_len);
    ctx->send_sge.length = data_len;
    ctx->send_wr.opcode = IBV_WR_SEND;
    ctx->send_wr.send_flags = IBV_SEND_SIGNALED;
    ctx->send_wr.next = NULL;
    ret = ibv_post_send(cm_id->qp, &ctx->send_wr, &bad_wr);
    if (ret < 0) {
        serverLog(LL_WARNING, "RDMA: post send failed, ret %d, error %d\n", ret, errno);

        conn->last_errno = errno;
        if (conn->state == CONN_STATE_CONNECTED)
            conn->state = CONN_STATE_ERROR;
        return C_ERR;
    }

    ctx->send_pending = data_len;

    return data_len;
}

static int connRdmaRead(connection *conn, void *buf, size_t buf_len) {
TRACE();
    int ret = 0;
    rdma_connection *rdma_conn = (rdma_connection *)conn;

    ret = (int)connRdmaHandleCq(rdma_conn, 0, buf, buf_len);
    if (!ret) {
        conn->state = CONN_STATE_CLOSED;
    } else if (ret < 0 && errno != EAGAIN) {
        conn->last_errno = errno;

        if (conn->state == CONN_STATE_CONNECTED)
            conn->state = CONN_STATE_ERROR;
    }

    return ret;
}

static ssize_t connRdmaSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
TRACE();
    UNUSED(conn);
    UNUSED(ptr);
    UNUSED(size);
    UNUSED(timeout);

    /* TODO to be implemented */
    serverNetError(server.neterr, "RDMA: syncwrite is to be implemented");
    return C_ERR;
}

static ssize_t connRdmaSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
TRACE();
    UNUSED(conn);
    UNUSED(ptr);
    UNUSED(size);
    UNUSED(timeout);
    /* TODO to be implemented */
    serverNetError(server.neterr, "RDMA: syncread is to be implemented");
    return C_ERR;
}

static ssize_t connRdmaSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
TRACE();
    UNUSED(conn);
    UNUSED(ptr);
    UNUSED(size);
    UNUSED(timeout);
    /* TODO to be implemented */
    serverNetError(server.neterr, "RDMA: syncreadline is to be implemented");
    return C_ERR;
}

static int connRdmaGetType(connection *conn) {
    UNUSED(conn);

    return CONN_TYPE_RDMA;
}

ConnectionType CT_RDMA = {
    .ae_handler = connRdmaEventHandler,
    .accept = connRdmaAccept,
    .set_read_handler = connRdmaSetReadHandler,
    .set_write_handler = connRdmaSetWriteHandler,
    .get_last_error = connRdmaGetLastError,
    .read = connRdmaRead,
    .write = connRdmaWrite,
    .close = connRdmaClose,
    .connect = connRdmaConnect,
    .blocking_connect = connRdmaBlockingConnect,
    .sync_read = connRdmaSyncRead,
    .sync_write = connRdmaSyncWrite,
    .sync_readline = connRdmaSyncReadLine,
    .get_type = connRdmaGetType
};

static int rdmaServer(char *err, int port, char *bindaddr, int af, int index)
{
    int s = ANET_OK, rv, afonly = 1;
    char _port[6];  /* strlen("65535") */
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage sock_addr;
    struct rdma_cm_id *listen_cmid;

    if (ibv_fork_init()) {
        serverNetError(err, "RDMA: ibv fork init error");
        return ANET_ERR;
    }

    snprintf(_port,6,"%d",port);
    memset(&hints,0,sizeof(hints));
    hints.ai_family = af;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;    /* No effect if bindaddr != NULL */
    if (bindaddr && !strcmp("*", bindaddr))
        bindaddr = NULL;

    if (af == AF_INET6 && bindaddr && !strcmp("::*", bindaddr))
        bindaddr = NULL;

    if ((rv = getaddrinfo(bindaddr, _port, &hints, &servinfo)) != 0) {
        serverNetError(err, "RDMA: %s", gai_strerror(rv));
        return ANET_ERR;
    } else if (!servinfo) {
        serverNetError(err, "RDMA: get addr info failed");
        s = ANET_ERR;
        goto end;
    }

    for (p = servinfo; p != NULL; p = p->ai_next) {
        memset(&sock_addr, 0, sizeof(sock_addr));
        if (p->ai_family == AF_INET6) {
            memcpy(&sock_addr, p->ai_addr, sizeof(struct sockaddr_in6));
            ((struct sockaddr_in6 *) &sock_addr)->sin6_family = AF_INET6;
            ((struct sockaddr_in6 *) &sock_addr)->sin6_port = htons(port);
        } else {
            memcpy(&sock_addr, p->ai_addr, sizeof(struct sockaddr_in));
            ((struct sockaddr_in *) &sock_addr)->sin_family = AF_INET;
            ((struct sockaddr_in *) &sock_addr)->sin_port = htons(port);
        }

        if (rdma_create_id(listen_channel, &listen_cmid, NULL, RDMA_PS_TCP)) {
            serverNetError(err, "RDMA: create listen cm id error");
            return ANET_ERR;
        }

        rdma_set_option(listen_cmid, RDMA_OPTION_ID, RDMA_OPTION_ID_AFONLY,
                        &afonly, sizeof(afonly));

        if (rdma_bind_addr(listen_cmid, (struct sockaddr *)&sock_addr)) {
            serverNetError(err, "RDMA: bind addr error");
            goto error;
        }

        if (rdma_listen(listen_cmid, 0)) {
            serverNetError(err, "RDMA: listen addr error");
            goto error;
        }

if (debug_rmda) serverLog(LL_WARNING, "listen: cmid %p\n", (void *)listen_cmid);	//TODO
        listen_cmids[index] = listen_cmid;
        goto end;
    }

error:
    if (listen_cmid)
        rdma_destroy_id(listen_cmid);
    s = ANET_ERR;

end:
    freeaddrinfo(servinfo);
    return s;
}

int listenToRdma(int port, socketFds *sfd) {
    int j, index = 0, ret;
    char **bindaddr = server.bindaddr;
    int bindaddr_count = server.bindaddr_count;
    char *default_bindaddr[2] = {"*", "-::*"};

    /* Force binding of 0.0.0.0 if no bind address is specified. */
    if (server.bindaddr_count == 0) {
        bindaddr_count = 2;
        bindaddr = default_bindaddr;
    }

    listen_channel = rdma_create_event_channel();
    if (!listen_channel) {
        serverLog(LL_WARNING, "RDMA: Could not create event channel");
        return C_ERR;
    }

    for (j = 0; j < bindaddr_count; j++) {
        char* addr = bindaddr[j];
        int optional = *addr == '-';

        if (optional)
            addr++;
        if (strchr(addr,':')) {
            /* Bind IPv6 address. */
            ret = rdmaServer(server.neterr, port, addr, AF_INET6, index);
        } else {
            /* Bind IPv4 address. */
            ret = rdmaServer(server.neterr, port, addr, AF_INET, index);
        }

        if (ret == ANET_ERR) {
            int net_errno = errno;
            serverLog(LL_WARNING, "RDMA: Could not create server for %s:%d: %s",
                      addr, port, server.neterr);

            if (net_errno == EADDRNOTAVAIL && optional)
                continue;

            if (net_errno == ENOPROTOOPT || net_errno == EPROTONOSUPPORT ||
                    net_errno == ESOCKTNOSUPPORT || net_errno == EPFNOSUPPORT ||
                    net_errno == EAFNOSUPPORT)
                continue;

            return C_ERR;
        }

        index++;
    }

    sfd->fd[sfd->count] = listen_channel->fd;
    anetNonBlock(NULL,sfd->fd[sfd->count]);
    anetCloexec(sfd->fd[sfd->count]);
    sfd->count++;

    return C_OK;
}

static int rdmaHandleConnect(char *err, struct rdma_cm_event *ev, char *ip, size_t ip_len, int *port)
{
TRACE();
    int ret = C_OK;
    struct rdma_cm_id *cm_id = ev->id;
    struct sockaddr_storage caddr;
    struct ibv_qp_init_attr init_attr;
    struct ibv_comp_channel *send_channel = NULL;
    struct ibv_comp_channel *recv_channel = NULL;
    struct ibv_cq *send_cq = NULL;
    struct ibv_cq *recv_cq = NULL;
    struct ibv_pd *pd = NULL;
    RdmaContext *ctx = NULL;
    struct rdma_conn_param conn_param = {
            .responder_resources = 1,
            .initiator_depth = 1,
            .retry_count = 5,
    };

    memcpy(&caddr, &cm_id->route.addr.dst_addr, sizeof(caddr));
    if (caddr.ss_family == AF_INET) {
        struct sockaddr_in *s = (struct sockaddr_in *)&caddr;
        if (ip)
            inet_ntop(AF_INET, (void*)&(s->sin_addr), ip, ip_len);
        if (port)
            *port = ntohs(s->sin_port);
    } else {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&caddr;
        if (ip)
            inet_ntop(AF_INET6, (void*)&(s->sin6_addr), ip, ip_len);
        if (port)
            *port = ntohs(s->sin6_port);
    }

    pd = ibv_alloc_pd(cm_id->verbs);
    if (!pd) {
        serverNetError(err, "RDMA: ibv alloc pd failed");
        return C_ERR;
    }

    if (debug_rmda) serverLog(LL_WARNING, "RDMA: pd %p\n", (void *)pd);	//TODO remove

    send_channel = ibv_create_comp_channel(cm_id->verbs);
    if (!send_channel) {
        serverNetError(err, "RDMA: ibv create send comp channel failed");
        goto free_rdma;
    }
            if (debug_rmda) serverLog(LL_WARNING, "RDMA: send comp_channel %p\n", (void *)send_channel);

    send_cq = ibv_create_cq(cm_id->verbs, 1, NULL, send_channel, 0);
    if (!send_cq) {
        serverNetError(err, "RDMA: ibv create cq failed");
        goto free_rdma;
    }
    if (debug_rmda) serverLog(LL_WARNING, "RDMA: send cq %p\n", (void *)send_cq);

    ibv_req_notify_cq(send_cq, 0);

    recv_channel = ibv_create_comp_channel(cm_id->verbs);
    if (!recv_channel) {
        serverNetError(err, "RDMA: ibv create recv comp channel failed");
        goto free_rdma;
    }
            if (debug_rmda) serverLog(LL_WARNING, "RDMA: recv comp_channel %p\n", (void *)recv_channel);

    recv_cq = ibv_create_cq(cm_id->verbs, 1, NULL, recv_channel, 0);
    if (!recv_cq) {
        serverNetError(err, "RDMA: ibv create cq failed");
        goto free_rdma;
    }
    if (debug_rmda) serverLog(LL_WARNING, "RDMA: recv cq %p\n", (void *)recv_cq);

    ibv_req_notify_cq(recv_cq, 0);


    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = 1;
    init_attr.cap.max_recv_wr = 1;
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = send_cq;
    init_attr.recv_cq = recv_cq;
    ret = rdma_create_qp(cm_id, pd, &init_attr);
    if (ret) {
        serverNetError(err, "RDMA: create qp failed");
        goto free_rdma;
    }

    ctx = zcalloc(sizeof(*ctx));
    ctx->send_pending = 0;
    ctx->send_handler = 0;
    ctx->send_channel = send_channel;
    ctx->recv_channel = recv_channel;
    ctx->send_cq = send_cq;
    ctx->recv_cq = recv_cq;
    ctx->pd = pd;
    if (rdmaSetupIoBuf(err, ctx, cm_id)) {
        goto free_rdma;
    }

    ret = rdma_accept(cm_id, &conn_param);
    if (ret) {
        serverNetError(err, "RDMA: accept failed");
        goto destroy_iobuf;
    }

    ctx->ip = zstrdup(ip);
    ctx->port = *port;
    cm_id->context = ctx;

    if (debug_rmda) serverLog(LL_WARNING, "RDMA: handle connect %s:%d\n", ctx->ip, ctx->port);//TODO notice
            //serverLog(LL_WARNING, "rdma DUMP comp_channel %p, recv comp_channel %p, pd %p, send cq %p, recv cq %p, context %p\n", cm_id->send_channel, cm_id->recv_cq_channel, cm_id->pd, cm_id->send_cq, cm_id->recv_cq, cm_id->context);	TODO remove
    return C_OK;

destroy_iobuf:
    rdmaDestroyIoBuf(ctx);
free_rdma:
    if (cm_id->qp)
        ibv_destroy_qp(cm_id->qp);
    if (send_cq)
        ibv_destroy_cq(send_cq);
    if (recv_cq)
        ibv_destroy_cq(recv_cq);
    if (send_channel)
        ibv_destroy_comp_channel(send_channel);
    if (recv_channel)
        ibv_destroy_comp_channel(recv_channel);
    if (pd)
        ibv_dealloc_pd(pd);

    /* reject connect request if hitting error */
    rdma_reject(cm_id, NULL, 0);

    return C_ERR;
}

static int rdmaHandleDisconnect(char *err, struct rdma_cm_event *ev)
{
TRACE();
    struct rdma_cm_id *cm_id = ev->id;
    RdmaContext *ctx = cm_id->context;
    connection *conn = ctx->conn;

    if (debug_rmda) serverLog(LL_WARNING, "RDMA: handle disconnect %s:%d\n", ctx->ip, ctx->port);//TODO notice
    UNUSED(err);
    ctx->cm_disconnected = true;

    /* kick connection read handler to avoid resource leak */
    callHandler(conn, conn->read_handler);

    return C_OK;
}

/*
 * rdmaAccept, actually it works as cm-event handler for listen cm_id.
 * accept a connection logic works in two steps:
 * 1, handle RDMA_CM_EVENT_CONNECT_REQUEST and return CM fd on success
 * 2, handle RDMA_CM_EVENT_ESTABLISHED and return C_OK on success
 */
int rdmaAccept(char *err, int s, char *ip, size_t ip_len, int *port, void **priv) {
TRACE();
    struct rdma_cm_event *ev;
    enum rdma_cm_event_type ev_type;
    int ret = C_OK;
    UNUSED(s);

    ret = rdma_get_cm_event(listen_channel, &ev);
    if (ret) {
        if (errno != EAGAIN) {
TRACE();
            serverLog(LL_WARNING, "rdma_get_cm_event failed, %s\n", strerror(errno));
        }
        return ANET_ERR;
    }

    ev_type = ev->event;
    if (debug_rmda) serverLog(LL_WARNING, "Handle event:cm_id %p, %d, %s\n", (void *)ev->id, ev_type, rdma_event_str(ev_type));    //TODO remove
    switch (ev_type) {
        case RDMA_CM_EVENT_CONNECT_REQUEST:
            ret = rdmaHandleConnect(err, ev, ip, ip_len, port);
            if (ret == C_OK) {
                RdmaContext *ctx = (RdmaContext *)ev->id->context;
                *priv = ev->id;
                ret = ctx->recv_channel->fd;
            }
            break;

        case RDMA_CM_EVENT_ESTABLISHED:
            ret = ANET_OK;
            break;

        case RDMA_CM_EVENT_CONNECT_ERROR:
        case RDMA_CM_EVENT_REJECTED:
        case RDMA_CM_EVENT_ADDR_CHANGE:
        case RDMA_CM_EVENT_DISCONNECTED:
            rdmaHandleDisconnect(err, ev);
            ret = C_OK;
            break;

        case RDMA_CM_EVENT_TIMEWAIT_EXIT:
            //iser_cm_timewait_exit(ev);
            break;

        case RDMA_CM_EVENT_MULTICAST_JOIN:
        case RDMA_CM_EVENT_MULTICAST_ERROR:
            serverLog(LL_NOTICE, "UD-related event:%d, %s - ignored\n",
                    ev_type, rdma_event_str(ev_type));
            break;

        case RDMA_CM_EVENT_DEVICE_REMOVAL:
            serverLog(LL_NOTICE, "Unsupported event:%d, %s - ignored\n",
                    ev_type, rdma_event_str(ev_type));
            break;

        case RDMA_CM_EVENT_ADDR_RESOLVED:
        case RDMA_CM_EVENT_ADDR_ERROR:
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
        case RDMA_CM_EVENT_ROUTE_ERROR:
        case RDMA_CM_EVENT_CONNECT_RESPONSE:
        case RDMA_CM_EVENT_UNREACHABLE:
            serverLog(LL_NOTICE, "Receive event:%d, %s - ignored\n",
                    ev_type, rdma_event_str(ev_type));
            break;

        default:
            serverLog(LL_NOTICE, "Illegal event:%d - ignored\n", ev_type);
            break;
    }

    if (rdma_ack_cm_event(ev)) {
        serverLog(LL_NOTICE, "ack cm event failed\n");
        return ANET_ERR;
    }

    return ret;
}

connection *connCreateRdma() {
    rdma_connection *conn = zcalloc(sizeof(rdma_connection));
    conn->c.type = &CT_RDMA;
    conn->c.fd = -1;

    return (connection *) conn;
}

connection *connCreateAcceptedRdma(int fd, void *priv) {
    rdma_connection *conn = (rdma_connection *)connCreateRdma();
    conn->c.fd = fd;
    conn->c.state = CONN_STATE_ACCEPTING;
    conn->cm_id = priv;

    return (connection *) conn;
}
#else    /* __linux__ */

"BUILD ERROR: RDMA is supported only on linux"

#endif   /* __linux__ */
#else    /* USE_RDMA */
int listenToRdma(int port, socketFds *sfd) {
    UNUSED(port);
    UNUSED(sfd);
    serverNetError(server.neterr, "RDMA: disabled, need rebuild with BUILD_RDMA");

    return C_ERR;
}

int rdmaAccept(char *err, int s, char *ip, size_t ip_len, int *port, void **priv) {
    UNUSED(err);
    UNUSED(s);
    UNUSED(ip);
    UNUSED(ip_len);
    UNUSED(port);
    UNUSED(priv);

    serverNetError(server.neterr, "RDMA: disabled, need rebuild with BUILD_RDMA");
    errno = EOPNOTSUPP;

    return C_ERR;
}

#endif
