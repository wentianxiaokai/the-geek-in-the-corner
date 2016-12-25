#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>

// #include "cma.h"
// #include "indexer.h"
#include <infiniband/driver.h>
#include <infiniband/marshall.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_cma_abi.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/ib.h>


struct cma_id_private {
  struct rdma_cm_id id;
  struct cma_device *cma_dev;
  void      *connect;
  size_t      connect_len;
  int     events_completed;
  int     connect_error;
  int     sync;
  pthread_cond_t    cond;
  pthread_mutex_t   mut;
  uint32_t    handle;
  struct cma_multicast  *mc_list;
  struct ibv_qp_init_attr *qp_init_attr;
  uint8_t     initiator_depth;
  uint8_t     responder_resources;
};



#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

const int BUFFER_SIZE = 102400;
const int TIMEOUT_IN_MS = 500; /* ms */

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct connection {
  struct rdma_cm_id *id;
  struct ibv_qp *qp;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;

  char *recv_region;
  char *send_region;

  int num_completions;
};

static void die(const char *reason);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void * poll_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);

static int on_addr_resolved(struct rdma_cm_id *id);
static void on_completion(struct ibv_wc *wc);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static int on_route_resolved(struct rdma_cm_id *id);

static struct context *s_ctx = NULL;

int main(int argc, char **argv)
{
  struct addrinfo *addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *conn= NULL;
  struct rdma_event_channel *ec = NULL;

  if (argc != 3)
    die("usage: client <server-address> <server-port>");

  TEST_NZ(getaddrinfo(argv[1], argv[2], NULL, &addr));
  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));
  freeaddrinfo(addr);

  while (rdma_get_cm_event(ec, &event) == 0) {

printf("client rdma_get_cm_event()  end \n");
// sleep(3);

    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
    {

printf("client on_event() break\n");
// sleep(3);


      break;
    }
    else
    {

printf("client on_event() ok\n");
// sleep(3);


    }
  }

printf("client rdma_destroy_event_channel() \n");
// sleep(3);

  rdma_destroy_event_channel(ec);

  return 0;
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 10;
  qp_attr->cap.max_recv_wr = 10;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
}

void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
      on_completion(&wc);
  }

  return NULL;
}

void post_receives(struct connection *conn)
{
  printf("client post_receives\n");
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)conn->recv_region;
  sge.length = BUFFER_SIZE;
  sge.lkey = conn->recv_mr->lkey;

  int err = ibv_post_recv(conn->qp, &wr, &bad_wr);
  printf("err:%d,%s\n", err,strerror(err));
  if( 0 != err )
    exit(-1);
  // TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void register_memory(struct connection *conn)
{
  conn->send_region = malloc(BUFFER_SIZE);
  conn->recv_region = malloc(BUFFER_SIZE);

  TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->send_region, 
    BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->recv_region, 
    BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
}

int on_addr_resolved(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct connection *conn;

  printf("client address resolved.\n");

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = conn = (struct connection *)malloc(sizeof(struct connection));

  conn->id = id;
  conn->qp = id->qp;
  conn->num_completions = 0;

  register_memory(conn);
  post_receives(conn);

  TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

  return 0;
}

void on_completion(struct ibv_wc *wc)
{
  printf(" \n\n\n -------------------------- \n");
  struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;

  printf("IBV_WC_SUCCESS:%d,wc->status:%d\n", IBV_WC_SUCCESS,wc->status);
  printf("IBV_WC_RECV:%d,IBV_WC_SEND:%d,,wc->opcode:%d\n", IBV_WC_RECV,IBV_WC_SEND,wc->opcode);

  if (wc->status != IBV_WC_SUCCESS)
    die("on_completion: status is not IBV_WC_SUCCESS.");

  if (wc->opcode & IBV_WC_RECV)
  {
    printf("client received message: %s\n", conn->recv_region);
    printf("conn->recv_region len:%d,wc->byte_len:%d\n", strlen(conn->recv_region),wc->byte_len);
  }
  else if (wc->opcode == IBV_WC_SEND)
  {
    printf("client send message: %s\n", conn->send_region);
    printf("client send completed successfully.\n");
  }
  else
    die("on_completion: completion isn't a send or a receive.");
  ++ conn->num_completions;
  printf("conn->num_completions:%d\n", conn->num_completions);
  if(conn->num_completions >= 2)
  {
      post_receives(conn);
    if( wc->opcode & IBV_WC_RECV)
    {
      struct ibv_send_wr wr, *bad_wr = NULL;
      struct ibv_sge sge;

      snprintf(conn->send_region  , BUFFER_SIZE , " ++++++++ ");

      printf("client connected. posting send...   22222222 \n");

      memset(&wr, 0, sizeof(wr));

      wr.wr_id = (uintptr_t)conn;
      wr.opcode = IBV_WR_SEND;
      wr.sg_list = &sge;
      wr.num_sge = 1;
      wr.send_flags = IBV_SEND_SIGNALED;

      sge.addr = (uintptr_t)(conn->send_region );
      // sge.length = 1 + strlen(conn->send_region);
      sge.length = BUFFER_SIZE ;
      sge.lkey = conn->send_mr->lkey;
      TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
    }


// // int rdma_notify(struct rdma_cm_id *id, enum ibv_event_type event)

//     // rdma_disconnect(conn->id);



//       // struct ucma_abi_get_event cmd;
//       // struct cma_id_private *id_priv;
//       // int ret;
//       // struct rdma_cm_id *id = conn->id;
//       // id_priv = container_of(id, struct cma_id_private, id);

//       // CMA_INIT_CMD(&cmd, sizeof cmd, ESTABLISHED);
//       // cmd.id = id_priv->handle;
//       // cmd.timeout_ms = 1000;

//       // ret = write(id->channel->fd, &cmd, sizeof cmd);
//       // if (ret != sizeof cmd)
//       //   return (ret >= 0) ? ERR(ENODATA) : -1;

//       // return ucma_complete(id);





  }
    printf("IBV_WC_SUCCESS:%d,wc->status:%d\n", IBV_WC_SUCCESS,wc->status);
  printf("IBV_WC_RECV:%d,IBV_WC_SEND:%d,,wc->opcode:%d\n", IBV_WC_RECV,IBV_WC_SEND,wc->opcode);
  printf(" -------------------------- \n\n\n");
}

int on_connection(void *context)
{
  struct connection *conn = (struct connection *)context;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

static int count = 0;
++ count;
// if( 1 == count )
{
  snprintf(conn->send_region, BUFFER_SIZE, "message from active/client side with pid %d", getpid());

  printf("client connected. posting send...\n");

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_region;
  // sge.length = 1 + strlen(conn->send_region);
  sge.length = BUFFER_SIZE;
  sge.lkey = conn->send_mr->lkey;

  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
  
}
// else
// {
//   struct ibv_send_wr wr, *bad_wr = NULL;
//   struct ibv_sge sge;

//   snprintf(conn->send_region, BUFFER_SIZE, " this is test: %4d%4d%4d%4d",count,count,count,count);

//   printf("client connected. posting send...   conn->send_region:%s \n",conn->send_region);

//   memset(&wr, 0, sizeof(wr));

//   wr.wr_id = (uintptr_t)conn;
//   wr.opcode = IBV_WR_SEND;
//   wr.sg_list = &sge;
//   wr.num_sge = 1;
//   wr.send_flags = IBV_SEND_SIGNALED;

//   sge.addr = (uintptr_t)conn->send_region;
//   // sge.length = 1 + strlen(conn->send_region);
//   sge.length = BUFFER_SIZE;
//   sge.lkey = conn->send_mr->lkey;
//   TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));

// // int rdma_notify(struct rdma_cm_id *id, enum ibv_event_type event)

//   // rdma_disconnect(conn->id);



//     // struct ucma_abi_get_event cmd;
//     // struct cma_id_private *id_priv;
//     // int ret;
//     // struct rdma_cm_id *id = conn->id;
//     // id_priv = container_of(id, struct cma_id_private, id);

//     // CMA_INIT_CMD(&cmd, sizeof cmd, ESTABLISHED);
//     // cmd.id = id_priv->handle;
//     // cmd.timeout_ms = 1000;

//     // ret = write(id->channel->fd, &cmd, sizeof cmd);
//     // if (ret != sizeof cmd)
//     //   return (ret >= 0) ? ERR(ENODATA) : -1;

//     // return ucma_complete(id);
// }

  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  struct connection *conn = (struct connection *)id->context;

  printf("client disconnected.\n");

  rdma_destroy_qp(id);

  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);

  free(conn->send_region);
  free(conn->recv_region);

  free(conn);

  rdma_destroy_id(id);

  return 1; /* exit event loop */
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;
  printf("RDMA_CM_EVENT_ADDR_RESOLVED:%d,RDMA_CM_EVENT_ROUTE_RESOLVED:%d,RDMA_CM_EVENT_ESTABLISHED:%d,RDMA_CM_EVENT_DISCONNECTED:%d\n", RDMA_CM_EVENT_ADDR_RESOLVED,RDMA_CM_EVENT_ROUTE_RESOLVED,RDMA_CM_EVENT_ESTABLISHED,RDMA_CM_EVENT_DISCONNECTED);
  printf("event->event:%d\n", event->event);
  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
    r = on_addr_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
    r = on_route_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}

int on_route_resolved(struct rdma_cm_id *id)
{
  struct rdma_conn_param cm_params;

  printf("client route resolved.\n");

  memset(&cm_params, 0, sizeof(cm_params));
  TEST_NZ(rdma_connect(id, &cm_params));

  return 0;
}
