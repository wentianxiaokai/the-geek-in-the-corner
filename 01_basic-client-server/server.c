#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

const int BUFFER_SIZE = 102400;

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct connection {
  struct ibv_qp *qp;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;

  char *recv_region;
  char *send_region;
};

static void die(const char *reason);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void * poll_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);

static void on_completion(struct ibv_wc *wc);
static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);

static struct context *s_ctx = NULL;

int main(int argc, char **argv)
{
#if _USE_IPV6
  struct sockaddr_in6 addr;
#else
  struct sockaddr_in addr;
#endif
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *listener = NULL;
  struct rdma_event_channel *ec = NULL;
  uint16_t port = 0;

  memset(&addr, 0, sizeof(addr));
#if _USE_IPV6
  addr.sin6_family = AF_INET6;
#else
  addr.sin_family = AF_INET;
#endif

printf("server rdma_create_event_channel() \n");
// sleep(3);


  TEST_Z(ec = rdma_create_event_channel());

printf("server rdma_create_id() \n");
// sleep(3);

  TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));

printf("server rdma_bind_addr() \n");
// sleep(3);

  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));

printf("server rdma_listen() \n");
// sleep(3);

  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */


printf("server rdma_get_src_port() \n");
// sleep(3);

  port = ntohs(rdma_get_src_port(listener));


  printf("server listening on port %d.\n", port);

  while (rdma_get_cm_event(ec, &event) == 0) {

printf("server rdma_get_cm_event()  end \n");
// sleep(3);

    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
    {

printf("server on_event() break \n");
// sleep(3);


      break;
    }
    else
    {

printf("server on_event() OK \n");
// sleep(3);

    }
  }

printf("server rdma_destroy_id() \n");
// sleep(3);


  rdma_destroy_id(listener);
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
  printf("\n *******   server posting receives\n");
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  printf(" 1111 \n");
  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  printf(" 222 \n");
printf("conn:%ld\n", conn);
  sge.addr = (uintptr_t)conn->recv_region;
  printf(" 333 \n");
  // sge.length = 1 + strlen(conn->send_region);
  sge.length = BUFFER_SIZE;
  sge.lkey = conn->recv_mr->lkey;
  printf(" 444 \n");

  TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
  printf(" 555 \n");

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

void on_completion(struct ibv_wc *wc)
{
    
  printf("IBV_WC_SUCCESS:%d,wc->status:%d\n", IBV_WC_SUCCESS,wc->status);
  printf("IBV_WC_RECV:%d,IBV_WC_SEND:%d,,wc->opcode:%d\n", IBV_WC_RECV,IBV_WC_SEND,wc->opcode);
  if (wc->status != IBV_WC_SUCCESS)
    die("on_completion: status is not IBV_WC_SUCCESS.");

  if (wc->opcode & IBV_WC_RECV) {
    struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;

    printf("server received message: %s,wc->byte_len:%d\n", conn->recv_region,wc->byte_len);

  } else if (wc->opcode == IBV_WC_SEND) {
    printf("server send completed successfully.\n");
  }



  static int count = 0;
  ++ count;
  if( count > 2  )
  {
      struct connection *conn2 = (struct connection *)(uintptr_t)wc->wr_id ;
      struct ibv_send_wr wr, *bad_wr = NULL;
      struct ibv_sge sge;
    if( wc->opcode & IBV_WC_RECV )
    {
      post_receives(conn2);

      snprintf(conn2->send_region, BUFFER_SIZE, "%4d%4d%4d%4d", count,count,count,count);

      printf("server connected. posting send...conn2->send_region: %s\n",conn2->send_region);

      memset(&wr, 0, sizeof(wr));

      wr.opcode = IBV_WR_SEND;
      wr.sg_list = &sge;
      wr.num_sge = 1;
      wr.send_flags = IBV_SEND_SIGNALED;

      sge.addr = (uintptr_t)conn2->send_region;
      // sge.length = 1 + strlen(conn2->send_region);
      sge.length = BUFFER_SIZE;
      sge.lkey = conn2->send_mr->lkey;

      printf("\n *******   server posting send\n");

      TEST_NZ(ibv_post_send(conn2->qp, &wr, &bad_wr));
      
    }



  }
}

int on_connect_request(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct rdma_conn_param cm_params;
  struct connection *conn;

  printf("server received connection request.\n");

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = conn = (struct connection *)malloc(sizeof(struct connection));
  conn->qp = id->qp;

  register_memory(conn);
  post_receives(conn);

  memset(&cm_params, 0, sizeof(cm_params));
  TEST_NZ(rdma_accept(id, &cm_params));

  return 0;
}

int on_connection(void *context)
{
  struct connection *conn = (struct connection *)context;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  post_receives(conn);
  snprintf(conn->send_region, BUFFER_SIZE, "message from passive/server side with pid %d", getpid());

  printf("server connected. posting send...\n");

  memset(&wr, 0, sizeof(wr));

  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_region;
  // sge.length = 1 + strlen(conn->send_region);
  sge.length = BUFFER_SIZE;
  sge.lkey = conn->send_mr->lkey;

  printf("\n *******   server posting send\n");

  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));

  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  struct connection *conn = (struct connection *)id->context;

  printf("server peer disconnected.\n");

  rdma_destroy_qp(id);

  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);

  free(conn->send_region);
  free(conn->recv_region);

  free(conn);

  rdma_destroy_id(id);

  return 0;
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;
printf("event->event:%d\n", event->event);
  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
    r = on_connect_request(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}

