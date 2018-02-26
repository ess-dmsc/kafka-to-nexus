#include "Consumer.h"
#include "logger.h"

namespace KafkaW {

PollStatus::~PollStatus() { reset(); }

PollStatus PollStatus::Ok() {
  PollStatus ret;
  ret.state = 0;
  return ret;
}

PollStatus PollStatus::Err() {
  PollStatus ret;
  ret.state = -1;
  return ret;
}

PollStatus PollStatus::EOP() {
  PollStatus ret;
  ret.state = -2;
  return ret;
}

PollStatus PollStatus::Empty() {
  PollStatus ret;
  ret.state = -3;
  return ret;
}

PollStatus PollStatus::make_Msg(std::unique_ptr<Msg> x) {
  PollStatus ret;
  ret.state = 1;
  ret.data = x.release();
  return ret;
}

PollStatus::PollStatus(PollStatus &&x)
    : state(std::move(x.state)), data(std::move(x.data)) {}

PollStatus &PollStatus::operator=(PollStatus &&x) {
  reset();
  std::swap(state, x.state);
  std::swap(data, x.data);
  return *this;
}

void PollStatus::reset() {
  if (state == 1) {
    if (auto x = (Msg *)data) {
      delete x;
    }
  }
  state = -1;
  data = nullptr;
}

PollStatus::PollStatus() {}

bool PollStatus::is_Ok() { return state == 0; }

bool PollStatus::is_Err() { return state == -1; }

bool PollStatus::is_EOP() { return state == -2; }

bool PollStatus::is_Empty() { return state == -3; }

std::unique_ptr<Msg> PollStatus::is_Msg() {
  if (state == 1) {
    std::unique_ptr<Msg> ret((Msg *)data);
    data = nullptr;
    return ret;
  }
  return nullptr;
}

static std::atomic<int> g_kafka_consumer_instance_count;

#define KERR(rk, err)                                                          \
  if (err != 0) {                                                              \
    LOG(Sev::Error, "Kafka {}  error: {}, {}, {}", rd_kafka_name(rk), err,     \
        rd_kafka_err2name((rd_kafka_resp_err_t)err),                           \
        rd_kafka_err2str((rd_kafka_resp_err_t)err));                           \
  }

Consumer::Consumer(BrokerSettings BrokerSettings)
    : ConsumerBrokerSettings(BrokerSettings) {
  init();
  id = g_kafka_consumer_instance_count++;
}

Consumer::~Consumer() {
  LOG(Sev::Debug, "~Consumer()");
  if (rk) {
    // commit offsets?
    if (0) {
      LOG(Sev::Debug, "rd_kafka_unsubscribe");
      rd_kafka_unsubscribe(rk);
    }
    if (0) {
      LOG(Sev::Debug, "rd_kafka_poll");
      int n1 = rd_kafka_poll(rk, 100);
      LOG(Sev::Debug, "  served {} reuests", n1);
    }
    if (1) {
      LOG(Sev::Debug, "rd_kafka_consumer_close");
      rd_kafka_consumer_close(rk);
    }
    // rd_kafka_consume_stop(rd_kafka_topic_t *, partition)  therefore low-level
    // API?
    if (1) {
      LOG(Sev::Debug, "rd_kafka_destroy");
      rd_kafka_destroy(rk);
      rk = nullptr;
    }
  }
  if (plist) {
    rd_kafka_topic_partition_list_destroy(plist);
    plist = nullptr;
  }
}

void Consumer::cb_log(rd_kafka_t const *rk, int level, char const *fac,
                      char const *buf) {
  auto self = reinterpret_cast<Consumer *>(rd_kafka_opaque(rk));
  LOG(Sev(level), "IID: {}  {}  fac: {}", self->id, buf, fac);
}

void Consumer::cb_error(rd_kafka_t *rk, int err_i, char const *msg,
                        void *opaque) {
  auto self = reinterpret_cast<Consumer *>(opaque);
  rd_kafka_resp_err_t err = (rd_kafka_resp_err_t)err_i;
  Sev ll = Sev::Debug;
  if (err == RD_KAFKA_RESP_ERR__TRANSPORT) {
    ll = Sev::Warning;
    // rd_kafka_dump(stdout, rk);
  }
  LOG(ll, "Kafka cb_error id: {}  broker: {}  errno: {}  errorname: {}  "
          "errorstring: {}  message: {}",
      self->id, self->ConsumerBrokerSettings.Address, err_i,
      rd_kafka_err2name(err), rd_kafka_err2str(err), msg);
}

int Consumer::cb_stats(rd_kafka_t *rk, char *json, size_t json_size,
                       void *opaque) {
  LOG(Sev::Debug, "INFO stats_cb {}  {:.{}}", json_size, json, json_size);
  // What does Kafka want us to return from this callback?
  return 0;
}

static void print_partition_list(rd_kafka_topic_partition_list_t *plist) {
  for (int i1 = 0; i1 < plist->cnt; ++i1) {
    auto &x = plist->elems[i1];
    LOG(Sev::Debug, "   {}  {}  {}", x.topic, x.partition, x.offset);
  }
}

void Consumer::cb_rebalance(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                            rd_kafka_topic_partition_list_t *plist,
                            void *opaque) {
  rd_kafka_resp_err_t err2;
  auto self = static_cast<Consumer *>(opaque);
  switch (err) {
  case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
    LOG(Sev::Debug, "cb_rebalance assign {}", rd_kafka_name(rk));
    if (auto &cb = self->on_rebalance_start) {
      cb(plist);
    }
    print_partition_list(plist);
    err2 = rd_kafka_assign(rk, plist);
    if (err2 != RD_KAFKA_RESP_ERR_NO_ERROR) {
      LOG(Sev::Notice, "rebalance error: {}  {}", rd_kafka_err2name(err2),
          rd_kafka_err2str(err2));
    }
    if (auto &cb = self->on_rebalance_assign) {
      cb(plist);
    }
    break;
  case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
    LOG(Sev::Warning, "cb_rebalance revoke:");
    print_partition_list(plist);
    err2 = rd_kafka_assign(rk, NULL);
    if (err2 != RD_KAFKA_RESP_ERR_NO_ERROR) {
      LOG(Sev::Warning, "rebalance error: {}  {}", rd_kafka_err2name(err2),
          rd_kafka_err2str(err2));
    }
    break;
  default:
    LOG(Sev::Info, "cb_rebalance failure and revoke: {}",
        rd_kafka_err2str(err));
    err2 = rd_kafka_assign(rk, NULL);
    if (err2 != RD_KAFKA_RESP_ERR_NO_ERROR) {
      LOG(Sev::Warning, "rebalance error: {}  {}", rd_kafka_err2name(err2),
          rd_kafka_err2str(err2));
    }
    break;
  }
}

void Consumer::init() {
  // librdkafka API sometimes wants to write errors into a buffer:
  int const errstr_N = 512;
  char errstr[errstr_N];

  auto conf = rd_kafka_conf_new();
  ConsumerBrokerSettings.apply(conf);

  rd_kafka_conf_set_log_cb(conf, Consumer::cb_log);
  rd_kafka_conf_set_error_cb(conf, Consumer::cb_error);
  rd_kafka_conf_set_stats_cb(conf, Consumer::cb_stats);
  rd_kafka_conf_set_rebalance_cb(conf, Consumer::cb_rebalance);
  rd_kafka_conf_set_consume_cb(conf, nullptr);
  rd_kafka_conf_set_opaque(conf, this);

  rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, errstr_N);
  if (!rk) {
    LOG(Sev::Error, "can not create kafka handle: {}", errstr);
    throw std::runtime_error("can not create Kafka handle");
  }

  rd_kafka_set_log_level(rk, 4);

  LOG(Sev::Info, "New Kafka consumer {} with brokers: {}", rd_kafka_name(rk),
      ConsumerBrokerSettings.Address.c_str());
  if (rd_kafka_brokers_add(rk, ConsumerBrokerSettings.Address.c_str()) == 0) {
    LOG(Sev::Error, "could not add brokers");
    throw std::runtime_error("could not add brokers");
  }

  rd_kafka_poll_set_consumer(rk);

  // Allocate some default size.  This is not a limit.
  plist = rd_kafka_topic_partition_list_new(16);
}

void Consumer::add_topic(std::string topic) {
  LOG(Sev::Info, "Consumer::add_topic  {}", topic);
  int partition = RD_KAFKA_PARTITION_UA;
  rd_kafka_topic_partition_list_add(plist, topic.c_str(), partition);
  int err = rd_kafka_subscribe(rk, plist);
  KERR(rk, err);
  if (err) {
    LOG(Sev::Error, "could not subscribe");
    throw std::runtime_error("can not subscribe");
  }
}

void Consumer::dump_current_subscription() {
  // Dump current subscription:
  rd_kafka_topic_partition_list_t *l1 = nullptr;
  rd_kafka_subscription(rk, &l1);
  if (l1) {
    for (int i1 = 0; i1 < l1->cnt; ++i1) {
      LOG(Sev::Info, "subscribed topics: {}  {}  off {}", l1->elems[i1].topic,
          rd_kafka_err2str(l1->elems[i1].err), l1->elems[i1].offset);
    }
    rd_kafka_topic_partition_list_destroy(l1);
  }
}

PollStatus Consumer::poll() {
  if (0)
    dump_current_subscription();
  if (0)
    rd_kafka_dump(stdout, rk);

  auto ret = PollStatus::Empty();

  auto msg = rd_kafka_consumer_poll(rk, ConsumerBrokerSettings.PollTimeoutMS);

  if (msg == nullptr) {
    return PollStatus::Empty();
  }

  static_assert(sizeof(char) == 1, "Failed: sizeof(char) == 1");
  std::unique_ptr<Msg> m2(new Msg);
  m2->kmsg = msg;
  if (msg->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    return PollStatus::make_Msg(std::move(m2));
  } else if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
    // Just an advisory.  msg contains which partition it is.
    return PollStatus::EOP();
  } else if (msg->err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
    LOG(Sev::Error, "RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN");
  } else if (msg->err == RD_KAFKA_RESP_ERR__BAD_MSG) {
    LOG(Sev::Error, "RD_KAFKA_RESP_ERR__BAD_MSG");
  } else if (msg->err == RD_KAFKA_RESP_ERR__DESTROY) {
    LOG(Sev::Error, "RD_KAFKA_RESP_ERR__DESTROY");
    // Broker will go away soon
  } else {
    LOG(Sev::Error, "unhandled msg error: {} {}", rd_kafka_err2name(msg->err),
        rd_kafka_err2str(msg->err));
  }
  return PollStatus::Err();
}
}
