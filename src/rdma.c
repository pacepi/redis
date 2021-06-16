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

static int rdma_debug = 0;
#define TRACE() if (rdma_debug){serverLog(LL_WARNING, "%s %d\n", __func__, __LINE__);}

#ifdef USE_RDMA
#ifdef __linux__    /* currently RDMA is only supported on Linux */
#include <assert.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <sys/types.h>
#include <sys/socket.h>

typedef enum RedisRdmaOpcode {
    RegisterLocalAddr,
} RedisRdmaOpcode;

typedef struct RedisRdmaCmd {
    uint8_t magic;
    uint8_t version;
    uint8_t opcode;
    uint8_t rsvd[5];
    uint64_t addr;
    uint32_t length;
    uint32_t key;
} RedisRdmaCmd;

#define MIN(a, b) (a) < (b) ? a : b
#define REDIS_MAX_SGE 1024
#define REDIS_RDMA_DEFAULT_RX_LEN  (1024*1024)
#define REDID_RDMA_CMD_MAGIC 'R'

typedef struct rdma_connection {
    connection c;
    struct rdma_cm_id *cm_id;
    int last_errno;
} rdma_connection;

typedef struct RdmaContext {
    connection *conn;
    char *ip;
    int port;
    struct ibv_pd *pd;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;

    /* TX */
    char *tx_addr;
    uint32_t tx_length;
    uint32_t tx_offset;
    uint32_t tx_key;
    char *send_buf;
    uint32_t send_length;
    uint32_t send_offset;
    uint32_t send_ops;
    struct ibv_mr *send_mr;

    /* RX */
    uint32_t rx_offset;
    char *recv_buf;
    unsigned int recv_length;
    unsigned int recv_offset;
    struct ibv_mr *recv_mr;

    /* CMD 0 ~ REDIS_MAX_SGE for recv buffer
     * REDIS_MAX_SGE ~ 2 * REDIS_MAX_SGE -1 for send buffer */
    RedisRdmaCmd *cmd_buf;
    struct ibv_mr *cmd_mr;
} RdmaContext;

static struct rdma_event_channel *listen_channel;
static struct rdma_cm_id *listen_cmids[CONFIG_BINDADDR_MAX];

static int rdmaPostRecv(RdmaContext *ctx, struct rdma_cm_id *cm_id, RedisRdmaCmd *cmd) {
    struct ibv_sge sge;
    size_t length = sizeof(RedisRdmaCmd);
    struct ibv_recv_wr recv_wr, *bad_wr;
    int ret;

    sge.addr = (uint64_t)cmd;
    sge.length = length;
    sge.lkey = ctx->cmd_mr->lkey;

    recv_wr.wr_id = (uint64_t)cmd;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;
    recv_wr.next = NULL;

    if (rdma_debug) serverLog(LL_WARNING, "RDMA: post cmd %p", (void *)cmd);
    ret = ibv_post_recv(cm_id->qp, &recv_wr, &bad_wr);
    if (ret && (ret != EAGAIN)) {
        serverLog(LL_WARNING, "RDMA: post recv failed: %d", ret);
        return C_ERR;
    }

    return C_OK;
}

static void rdmaDestroyIoBuf(RdmaContext *ctx)
{
TRACE();
    if (ctx->recv_mr) {
        ibv_dereg_mr(ctx->recv_mr);
        ctx->recv_mr = NULL;
    }

    zfree(ctx->recv_buf);
    ctx->recv_buf = NULL;

    if (ctx->send_mr) {
        ibv_dereg_mr(ctx->send_mr);
        ctx->send_mr = NULL;
    }

    zfree(ctx->send_buf);
    ctx->send_buf = NULL;

    if (ctx->cmd_mr) {
        ibv_dereg_mr(ctx->cmd_mr);
        ctx->cmd_mr = NULL;
    }

    zfree(ctx->cmd_buf);
    ctx->cmd_buf = NULL;
}

static int rdmaSetupIoBuf(char *err, RdmaContext *ctx, struct rdma_cm_id *cm_id)
{
TRACE();
    int access = IBV_ACCESS_LOCAL_WRITE;
    size_t length = sizeof(RedisRdmaCmd) * REDIS_MAX_SGE * 2;
    RedisRdmaCmd *cmd;
    int i;

    /* setup CMD buf & MR */
    ctx->cmd_buf = zcalloc(length);
    ctx->cmd_mr = ibv_reg_mr(ctx->pd, ctx->cmd_buf, length, access);
    if (!ctx->cmd_mr) {
        serverNetError(err, "RDMA: reg mr for CMD failed");
	goto destroy_iobuf;
    }

    for (i = 0; i < REDIS_MAX_SGE; i++) {
        cmd = ctx->cmd_buf + i;

        if (rdmaPostRecv(ctx, cm_id, cmd) == C_ERR) {
            serverNetError(err, "RDMA: post recv failed");
            goto destroy_iobuf;
        }
    }

    /* setup recv buf & MR */
    access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    length = REDIS_RDMA_DEFAULT_RX_LEN;
    ctx->recv_buf = zcalloc(length);
    ctx->recv_length = length;
    ctx->recv_mr = ibv_reg_mr(ctx->pd, ctx->recv_buf, length, access);
    if (!ctx->recv_mr) {
        serverNetError(err, "RDMA: reg mr for recv buffer failed");
	goto destroy_iobuf;
    }

    return C_OK;

destroy_iobuf:
    rdmaDestroyIoBuf(ctx);
    return C_ERR;
}

static int rdmaAdjustSendbuf(RdmaContext *ctx, unsigned int length) {
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

    if (length == ctx->send_length) {
        return C_OK;
    }

    /* try to free old MR & buffer */
    if (ctx->send_length) {
        ibv_dereg_mr(ctx->send_mr);
        zfree(ctx->send_buf);
        ctx->send_length = 0;
    }

    /* create a new buffer & MR */
    ctx->send_buf = zcalloc(length);
    ctx->send_length = length;
    ctx->send_mr = ibv_reg_mr(ctx->pd, ctx->send_buf, length, access);
    if (!ctx->send_mr) {
        serverNetError(server.neterr, "RDMA: reg send mr failed");
        zfree(ctx->send_buf);
        ctx->send_buf = NULL;
        ctx->send_length = 0;
        return C_ERR;
    }

if (rdma_debug) serverLog(LL_WARNING, "RDMA: adjust send buf %p, len %d", ctx->send_buf, ctx->send_length);
    return C_OK;
}

static int rdmaSendCommand(RdmaContext *ctx, struct rdma_cm_id *cm_id, RedisRdmaCmd *cmd) {
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    RedisRdmaCmd *_cmd;
    int i, ret;

TRACE();

    /* find an unused cmd buffer */
    for (i = REDIS_MAX_SGE; i < 2 * REDIS_MAX_SGE; i++) {
        _cmd = ctx->cmd_buf + i;
        if (!_cmd->magic) {
            break;
        }
    }

    assert(i < 2 * REDIS_MAX_SGE);

    _cmd->addr = htonu64(cmd->addr);
    _cmd->length = htonl(cmd->length);
    _cmd->key = htonl(cmd->key);
    _cmd->opcode = cmd->opcode;
    _cmd->magic = REDID_RDMA_CMD_MAGIC;

    sge.addr = (uint64_t)_cmd;
    sge.length = sizeof(RedisRdmaCmd);
    sge.lkey = ctx->cmd_mr->lkey;

    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.wr_id = (uint64_t)_cmd;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.next = NULL;
    ret = ibv_post_send(cm_id->qp, &send_wr, &bad_wr);
    if (ret) {
        serverLog(LL_WARNING, "RDMA: post send failed: %d", ret);
        return C_ERR;
    }

    if (rdma_debug) serverLog(LL_WARNING, "RDMA: SEND cmd addr %p, length %d, key 0x%x, opcode %d\n", (void *)cmd->addr, cmd->length, cmd->key, cmd->opcode);

    return C_OK;
}

static int connRdmaRegisterRx(RdmaContext *ctx, struct rdma_cm_id *cm_id) {
    RedisRdmaCmd cmd;

TRACE();
    cmd.addr = (uint64_t)ctx->recv_buf;
    cmd.length = ctx->recv_length;
    cmd.key = ctx->recv_mr->rkey;
    cmd.opcode = RegisterLocalAddr;

    ctx->rx_offset = 0;
    ctx->recv_offset = 0;

    return rdmaSendCommand(ctx, cm_id, &cmd);
}

static int connRdmaHandleRecv(RdmaContext *ctx, struct rdma_cm_id *cm_id, RedisRdmaCmd *cmd, uint32_t byte_len) {
    RedisRdmaCmd _cmd;

    if (unlikely(byte_len != sizeof(RedisRdmaCmd))) {
        serverLog(LL_WARNING, "RDMA: FATAL error, recv corrupted cmd");
        return C_ERR;
    }

    _cmd.addr = ntohu64(cmd->addr);
    _cmd.length = ntohl(cmd->length);
    _cmd.key = ntohl(cmd->key);
    _cmd.opcode = cmd->opcode;
    if (rdma_debug) serverLog(LL_WARNING, "RDMA: RECV CMD addr 0x%lx, length 0x%x, opcode %d", _cmd.addr, _cmd.length, _cmd.opcode);

    switch (_cmd.opcode) {
    case RegisterLocalAddr:
        ctx->tx_addr = (char *)_cmd.addr;
        ctx->tx_length = _cmd.length;
        ctx->tx_key = _cmd.key;
        ctx->tx_offset = 0;
        rdmaAdjustSendbuf(ctx, ctx->tx_length);

        break;

    default:
        serverLog(LL_WARNING, "RDMA: FATAL error, unknown cmd");
        return C_ERR;
    }

    return rdmaPostRecv(ctx, cm_id, cmd);
}

static int connRdmaHandleSend(RedisRdmaCmd *cmd) {
    /* mark this cmd has already sent */
    cmd->magic = 0;

    return C_OK;
}

static int connRdmaHandleRecvImm(RdmaContext *ctx, struct rdma_cm_id *cm_id, RedisRdmaCmd *cmd, uint32_t byte_len) {
    if (rdma_debug) serverLog(LL_WARNING, "RDMA: Handle recv imm[%d], rx offset %d, recv_offset %d, recv_length %d\n", byte_len, ctx->rx_offset, ctx->recv_offset, ctx->recv_length);
    assert(byte_len + ctx->rx_offset <= ctx->recv_length);

    ctx->rx_offset += byte_len;

    return rdmaPostRecv(ctx, cm_id, cmd);
}

static int connRdmaHandleWrite(RdmaContext *ctx, uint32_t byte_len) {
    if (rdma_debug) serverLog(LL_WARNING, "RDMA: Handle write[%d], tx offset %d, tx length %d\n", byte_len, ctx->tx_offset, ctx->tx_length);

    return C_OK;
}


static int connRdmaHandleCq(rdma_connection *rdma_conn) {
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;
    struct ibv_wc wc = {0};
    RedisRdmaCmd *cmd;
    int ret;

TRACE();
    if (ibv_get_cq_event(ctx->comp_channel, &ev_cq, &ev_ctx) < 0) {
        if (errno != EAGAIN) {
            serverLog(LL_WARNING, "RDMA: get CQ event error");
            return C_ERR;
        }
    } else if (ibv_req_notify_cq(ev_cq, 0)) {
        serverLog(LL_WARNING, "RDMA: notify CQ error");
        return C_ERR;
    }

pollcq:
    ret = ibv_poll_cq(ctx->cq, 1, &wc);
    if (ret < 0) {
        serverLog(LL_WARNING, "RDMA: poll recv CQ error");
        return C_ERR;
    } else if (ret == 0) {
        return C_OK;
    }

    ibv_ack_cq_events(ctx->cq, 1);

    if (rdma_debug) serverLog(LL_WARNING, "status 0x%x, opcode 0x%x, byte_len %d wr_id 0x%lx\n", wc.status, wc.opcode, wc.byte_len, wc.wr_id);

    if (wc.status != IBV_WC_SUCCESS) {
        serverLog(LL_WARNING, "RDMA: CQ handle error status 0x%x\n", wc.status);
        return C_ERR;
    }

    switch (wc.opcode) {
    case IBV_WC_RECV:
        if (rdma_debug) serverLog(LL_WARNING, "RDMA: RECV [%d]", wc.byte_len);

        cmd = (RedisRdmaCmd *)wc.wr_id;
        if (connRdmaHandleRecv(ctx, cm_id, cmd, wc.byte_len) == C_ERR) {
            return C_ERR;
        }
        break;

    case IBV_WC_RECV_RDMA_WITH_IMM:
        if (rdma_debug) serverLog(LL_WARNING, "RDMA: RECV IMM [%d] IMM %d, WR_ID 0x%lx", wc.byte_len, wc.wc_flags & IBV_WC_WITH_IMM, wc.wr_id);
        cmd = (RedisRdmaCmd *)wc.wr_id;
        if (connRdmaHandleRecvImm(ctx, cm_id, cmd, wc.byte_len) == C_ERR) {
            rdma_conn->c.state = CONN_STATE_ERROR;
            return C_ERR;
        }

        break;
    case IBV_WC_RDMA_WRITE:
        if (rdma_debug) serverLog(LL_WARNING, "RDMA: WRITE [%d] IMM %d, WR_ID 0x%lx", wc.byte_len, wc.wc_flags & IBV_WC_WITH_IMM, wc.wr_id);
        if (connRdmaHandleWrite(ctx, wc.byte_len) == C_ERR) {
            return C_ERR;
        }

        break;

    case IBV_WC_SEND:
        cmd = (RedisRdmaCmd *)wc.wr_id;
        if (connRdmaHandleSend(cmd) == C_ERR) {
            return C_ERR;
        }

        if (rdma_debug) serverLog(LL_WARNING, "RDMA: SEND [%d]", wc.byte_len);
        break;

    default:
        serverLog(LL_WARNING, "RDMA: unexpected opcode 0x[%x]", wc.opcode);
        return C_ERR;
    }

    goto pollcq;
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

static int connRdmaSetRwHandler(connection *conn) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;

TRACE();
    /* save conn into RdmaContext */
    ctx->conn = conn;

    /* IB channel only has POLLIN event */
    if (conn->read_handler || conn->write_handler) {
        if (aeCreateFileEvent(server.el, conn->fd, AE_READABLE, conn->type->ae_handler, conn) == AE_ERR) {
            return C_ERR;
        }
    } else {
        aeDeleteFileEvent(server.el, conn->fd, AE_READABLE);
    }

    return C_OK;
}

static int connRdmaSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
TRACE();
    conn->write_handler = func;
    if (barrier) {
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    } else {
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;
    }

    return connRdmaSetRwHandler(conn);
}

static int connRdmaSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
TRACE();
    conn->read_handler = func;

    return connRdmaSetRwHandler(conn);
}

static void connRdmaEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask) {
TRACE();
    rdma_connection *rdma_conn = (rdma_connection *)clientData;
    connection *conn = &rdma_conn->c;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;
    int ret = 0;

    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    ret = connRdmaHandleCq(rdma_conn);
    if (ret == C_ERR) {
        conn->state = CONN_STATE_ERROR;
        return;
    }

    /* uplayer should read all */
    while (ctx->recv_offset < ctx->rx_offset) {
        if (conn->read_handler) {
            callHandler(conn, conn->read_handler);
        }
    }

    /* recv buf is full, register a new RX buffer */
    if (ctx->recv_offset == ctx->recv_length) {
        connRdmaRegisterRx(ctx, cm_id);
    }

    /* TX buffer has been refreshed, try to send remaining buffer */
    if (!ctx->tx_offset) {
        if (conn->write_handler) {
            callHandler(conn, conn->write_handler);
        }
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
printf("connRdmaClose\n");
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;

    if (conn->fd != -1) {
        aeDeleteFileEvent(server.el, conn->fd, AE_READABLE | AE_WRITABLE);
        conn->fd = -1;
    }

    connRdmaHandleCq(rdma_conn);
    rdma_disconnect(cm_id);
    ibv_destroy_cq(ctx->cq);
    rdmaDestroyIoBuf(ctx);
    ibv_destroy_qp(cm_id->qp);
    ibv_destroy_comp_channel(ctx->comp_channel);
    ibv_dealloc_pd(ctx->pd);
    rdma_destroy_id(cm_id);

    zfree(conn);
}

static size_t connRdmaSend(connection *conn, const void *data, size_t data_len) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    uint32_t off = ctx->tx_offset;
    char *addr = ctx->send_buf + off;
    char *remote_addr = ctx->tx_addr + ctx->tx_offset;
    int ret;

    memcpy(addr, data, data_len);

    sge.addr = (uint64_t)addr;
    sge.lkey = ctx->send_mr->lkey;
    sge.length = data_len;

    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    send_wr.send_flags = (++ctx->send_ops % REDIS_MAX_SGE) ? 0 : IBV_SEND_SIGNALED;
    send_wr.imm_data = htonl(0);
    send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
    send_wr.wr.rdma.rkey = ctx->tx_key;
    send_wr.wr_id = 0;
    send_wr.next = NULL;
    ret = ibv_post_send(cm_id->qp, &send_wr, &bad_wr);
    if (ret) {
        serverLog(LL_WARNING, "RDMA: post send failed: %d", ret);
        conn->state = CONN_STATE_ERROR;
        return C_ERR;
    }
    if (rdma_debug) serverLog(LL_WARNING, "RDMA: SEND TX addr %p, offset %d, key 0x%x, length %ld, send addr %p", ctx->tx_addr, ctx->tx_offset, ctx->tx_key, data_len, ctx->send_buf);

    ctx->tx_offset += data_len;

    return data_len;
}

static int connRdmaWrite(connection *conn, const void *data, size_t data_len) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;
    uint32_t towrite;

TRACE();
    if (conn->state == CONN_STATE_ERROR || conn->state == CONN_STATE_CLOSED) {
        return C_ERR;
    }

    assert(ctx->tx_offset <= ctx->tx_length);
    towrite = MIN(ctx->tx_length - ctx->tx_offset, data_len);
    if (!towrite) {
        return 0;
    }

    //if (rdma_debug) serverLog(LL_WARNING, "RDMA: write data len[%ld] %s", data_len, (char *)data);
    if (rdma_debug) serverLog(LL_WARNING, "RDMA: write data len[%ld]", data_len);

    return connRdmaSend(conn, data, towrite);
}

static int connRdmaRead(connection *conn, void *buf, size_t buf_len) {
TRACE();
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;
    uint32_t toread;

    if (conn->state == CONN_STATE_ERROR || conn->state == CONN_STATE_CLOSED) {
        return -1;
    }

    assert(ctx->recv_offset < ctx->rx_offset);
    toread = MIN(ctx->rx_offset - ctx->recv_offset, buf_len);

    assert(ctx->recv_offset + toread <= ctx->recv_length);
    memcpy(buf, ctx->recv_buf + ctx->recv_offset, toread);

    //if (rdma_debug) serverLog(LL_WARNING, "RDMA: toread[%d] buf_len[%ld] offset[%d] %s", toread, buf_len, ctx->recv_offset, (char *)buf);
    if (rdma_debug) serverLog(LL_WARNING, "RDMA: toread[%d] buf_len[%ld] offset[%d]", toread, buf_len, ctx->recv_offset);

    ctx->recv_offset += toread;

    return toread;
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

if (rdma_debug) serverLog(LL_WARNING, "listen: cmid %p\n", (void *)listen_cmid);	//TODO
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

    assert(server.proto_max_bulk_len <= 512ll * 1024 * 1024);

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
    struct ibv_comp_channel *comp_channel = NULL;
    struct ibv_cq *cq = NULL;
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

    if (rdma_debug) serverLog(LL_WARNING, "RDMA: pd %p\n", (void *)pd);	//TODO remove

    comp_channel = ibv_create_comp_channel(cm_id->verbs);
    if (!comp_channel) {
        serverNetError(err, "RDMA: ibv create send comp channel failed");
        goto free_rdma;
    }

    cq = ibv_create_cq(cm_id->verbs, REDIS_MAX_SGE * 2, NULL, comp_channel, 0);
    if (!cq) {
        serverNetError(err, "RDMA: ibv create cq failed");
        goto free_rdma;
    }
    if (rdma_debug) serverLog(LL_WARNING, "RDMA: send cq %p\n", (void *)cq);

    ibv_req_notify_cq(cq, 0);

    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = REDIS_MAX_SGE;
    init_attr.cap.max_recv_wr = REDIS_MAX_SGE;
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = cq;
    init_attr.recv_cq = cq;
    ret = rdma_create_qp(cm_id, pd, &init_attr);
    if (ret) {
        serverNetError(err, "RDMA: create qp failed");
        goto free_rdma;
    }

    ctx = zcalloc(sizeof(*ctx));
    ctx->comp_channel = comp_channel;
    ctx->cq = cq;
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

    if (rdma_debug) serverLog(LL_WARNING, "RDMA: handle connect %s:%d\n", ctx->ip, ctx->port);//TODO notice
    return C_OK;

destroy_iobuf:
    rdmaDestroyIoBuf(ctx);
free_rdma:
    if (cm_id->qp)
        ibv_destroy_qp(cm_id->qp);
    if (cq)
        ibv_destroy_cq(cq);
    if (comp_channel)
        ibv_destroy_comp_channel(comp_channel);
    if (pd)
        ibv_dealloc_pd(pd);

    /* reject connect request if hitting error */
    rdma_reject(cm_id, NULL, 0);

    return C_ERR;
}

static int rdmaHandleEstablished(char *err, struct rdma_cm_event *ev)
{
TRACE();
    struct rdma_cm_id *cm_id = ev->id;
    RdmaContext *ctx = cm_id->context;

    connRdmaRegisterRx(ctx, cm_id);

    if (rdma_debug) serverLog(LL_WARNING, "RDMA: handle establisted %s:%d\n", ctx->ip, ctx->port);//TODO notice
    UNUSED(err);

    return C_OK;
}

static int rdmaHandleDisconnect(char *err, struct rdma_cm_event *ev)
{
TRACE();
    struct rdma_cm_id *cm_id = ev->id;
    RdmaContext *ctx = cm_id->context;
    connection *conn = ctx->conn;

    if (rdma_debug) serverLog(LL_WARNING, "RDMA: handle disconnect %s:%d\n", ctx->ip, ctx->port);//TODO notice
    UNUSED(err);
    conn->state = CONN_STATE_CLOSED;

    /* kick connection read/write handler to avoid resource leak */
    if (conn->read_handler) {
        callHandler(conn, conn->read_handler);
    } else if (conn->write_handler) {
        callHandler(conn, conn->write_handler);
    }

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
    if (rdma_debug) serverLog(LL_WARNING, "Handle event:cm_id %p, %d, %s\n", (void *)ev->id, ev_type, rdma_event_str(ev_type));    //TODO remove
    switch (ev_type) {
        case RDMA_CM_EVENT_CONNECT_REQUEST:
            ret = rdmaHandleConnect(err, ev, ip, ip_len, port);
            if (ret == C_OK) {
                RdmaContext *ctx = (RdmaContext *)ev->id->context;
                *priv = ev->id;
                ret = ctx->comp_channel->fd;
            }
            break;

        case RDMA_CM_EVENT_ESTABLISHED:
            ret = rdmaHandleEstablished(err, ev);
            break;

        case RDMA_CM_EVENT_CONNECT_ERROR:
        case RDMA_CM_EVENT_REJECTED:
        case RDMA_CM_EVENT_ADDR_CHANGE:
        case RDMA_CM_EVENT_DISCONNECTED:
            rdmaHandleDisconnect(err, ev);
            ret = C_OK;
            break;

        case RDMA_CM_EVENT_TIMEWAIT_EXIT:
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

"BUILD ERROR: RDMA is only supported on linux"

#endif   /* __linux__ */
#else    /* USE_RDMA */
int listenToRdma(int port, socketFds *sfd) {
    UNUSED(port);
    UNUSED(sfd);
    serverNetError(server.neterr, "RDMA: disabled, need rebuild with BUILD_RDMA=yes");

    return C_ERR;
}

int rdmaAccept(char *err, int s, char *ip, size_t ip_len, int *port, void **priv) {
    UNUSED(err);
    UNUSED(s);
    UNUSED(ip);
    UNUSED(ip_len);
    UNUSED(port);
    UNUSED(priv);

    serverNetError(server.neterr, "RDMA: disabled, need rebuild with BUILD_RDMA=yes");
    errno = EOPNOTSUPP;

    return C_ERR;
}

#endif
