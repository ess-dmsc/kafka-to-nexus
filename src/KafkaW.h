#pragma once
#include <atomic>
#include <functional>
#include <librdkafka/rdkafka.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace KafkaW {
// Want to expose this typedef also for users of this namespace
using uchar = unsigned char;
}

#if HAVE_KAFKAW_INSPECT
#include "KafkaW-inspect.h"
#endif

namespace KafkaW {

/// POD to collect the options
class BrokerOpt {
public:
  BrokerOpt();
  void apply(rd_kafka_conf_t *conf);
  std::string address;
  int poll_timeout_ms = 100;
  std::map<std::string, int> conf_ints;
  std::map<std::string, std::string> conf_strings;
};

class TopicOpt {
public:
  TopicOpt();
  void apply(rd_kafka_topic_conf_t *conf);
  std::map<std::string, int> conf_ints;
  std::map<std::string, std::string> conf_strings;
};

class Msg {
public:
  ~Msg();
  uchar *data();
  uint32_t size();
  void *kmsg;
  char const *topic_name();
  int32_t offset();
  int32_t partition();
};

class PollStatus {
public:
  static PollStatus Ok();
  static PollStatus Err();
  static PollStatus EOP();
  static PollStatus Empty();
  static PollStatus make_Msg(std::unique_ptr<Msg> x);
  PollStatus(PollStatus &&);
  PollStatus &operator=(PollStatus &&);
  ~PollStatus();
  void reset();
  PollStatus();
  bool is_Ok();
  bool is_Err();
  bool is_EOP();
  bool is_Empty();
  std::unique_ptr<Msg> is_Msg();

private:
  int state = -1;
  void *data = nullptr;
};

class Inspect;

class Consumer {
public:
  Consumer(BrokerOpt opt);
  Consumer(Consumer &&) = delete;
  Consumer(Consumer const &) = delete;
  ~Consumer();
  void init();
  void add_topic(std::string topic);
  void dump_current_subscription();
  PollStatus poll();
  std::function<void(rd_kafka_topic_partition_list_t *plist)>
      on_rebalance_assign;
  std::function<void(rd_kafka_topic_partition_list_t *plist)>
      on_rebalance_start;
  rd_kafka_t *rk = nullptr;

private:
  BrokerOpt opt;
  static void cb_log(rd_kafka_t const *rk, int level, char const *fac,
                     char const *buf);
  static int cb_stats(rd_kafka_t *rk, char *json, size_t json_size,
                      void *opaque);
  static void cb_error(rd_kafka_t *rk, int err_i, char const *reason,
                       void *opaque);
  static void cb_rebalance(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                           rd_kafka_topic_partition_list_t *plist,
                           void *opaque);
  static void cb_consume(rd_kafka_message_t *msg, void *opaque);
  rd_kafka_topic_partition_list_t *plist = nullptr;
  int id = 0;
};

class ProducerTopic;

class ProducerMsg {
public:
  virtual ~ProducerMsg();
  virtual void delivery_ok();
  virtual void delivery_fail();
  uchar *data;
  uint32_t size;
};

struct ProducerStats {
  std::atomic<uint64_t> produced{0};
  std::atomic<uint32_t> produce_fail{0};
  std::atomic<uint32_t> local_queue_full{0};
  std::atomic<uint64_t> produce_cb{0};
  std::atomic<uint64_t> produce_cb_fail{0};
  std::atomic<uint64_t> poll_served{0};
  std::atomic<uint64_t> msg_too_large{0};
  std::atomic<uint64_t> produced_bytes{0};
  std::atomic<uint32_t> out_queue{0};
  ProducerStats();
  ProducerStats(ProducerStats const &);
};

class Producer {
public:
  typedef ProducerTopic Topic;
  typedef ProducerMsg Msg;
  typedef ProducerStats Stats;
  Producer(BrokerOpt opt);
  Producer(Producer const &) = delete;
  Producer(Producer &&x);
  ~Producer();
  void poll_while_outq();
  void poll();
  uint64_t total_produced();
  uint64_t outq();
  static void cb_delivered(rd_kafka_t *rk, rd_kafka_message_t const *msg,
                           void *opaque);
  static void cb_error(rd_kafka_t *rk, int err_i, char const *reason,
                       void *opaque);
  static int cb_stats(rd_kafka_t *rk, char *json, size_t json_len,
                      void *opaque);
  static void cb_log(rd_kafka_t const *rk, int level, char const *fac,
                     char const *buf);
  static void cb_throttle(rd_kafka_t *rk, char const *broker_name,
                          int32_t broker_id, int throttle_time_ms,
                          void *opaque);
  rd_kafka_t *rd_kafka_ptr() const;
  std::function<void(rd_kafka_message_t const *msg)> on_delivery_ok;
  std::function<void(rd_kafka_message_t const *msg)> on_delivery_failed;
  std::function<void(Producer *, rd_kafka_resp_err_t)> on_error;
  // Currently it's nice to have acces to these two for statistics:
  BrokerOpt opt;
  rd_kafka_t *rk = nullptr;
  std::atomic<uint64_t> total_produced_{0};
  Stats stats;

private:
  int id = 0;

public:
#if HAVE_KAFKAW_INSPECT
  unique_ptr<Inspect> inspect();
#endif
};

class ProducerTopic {
public:
  ProducerTopic(ProducerTopic &&);
  ProducerTopic(std::shared_ptr<Producer> producer, std::string name);
  ~ProducerTopic();
  int produce(uchar *msg_data, size_t msg_size, bool print_err = false);
  int produce(std::unique_ptr<Producer::Msg> &msg);
  // Currently it's nice to have access to these for statistics:
  std::shared_ptr<Producer> producer;
  rd_kafka_topic_t *rkt = nullptr;
  void do_copy();

private:
  std::string _name;
  bool _do_copy{false};
};
}
