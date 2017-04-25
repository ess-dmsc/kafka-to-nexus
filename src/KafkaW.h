#pragma once
#include <functional>
#include <librdkafka/rdkafka.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace KafkaW {

using uchar = uint8_t;

/// POD to collect the options
class BrokerOpt {
public:
  BrokerOpt();
  void apply(rd_kafka_conf_t *conf);
  std::string address;
  int poll_timeout_ms = 10;
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
  static PollStatus make_Msg(std::unique_ptr<Msg> x);
  PollStatus(PollStatus &&);
  PollStatus &operator=(PollStatus &&);
  ~PollStatus();
  void reset();
  PollStatus();
  bool is_Ok();
  bool is_Err();
  std::unique_ptr<Msg> is_Msg();

private:
  int state = -1;
  void *data = nullptr;
};

class Consumer {
public:
  Consumer(BrokerOpt opt);
  ~Consumer();
  void start();
  void add_topic(std::string topic);
  void dump_current_subscription();
  PollStatus poll();
  std::function<void()> *on_rebalance_assign = nullptr;
  std::function<void(rd_kafka_topic_partition_list_t *plist)>
  on_rebalance_start = nullptr;
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
  // rd_kafka_topic_t * rkt = nullptr;
  rd_kafka_topic_partition_list_t *plist = nullptr;
  int id = 0;
};

class ProducerTopic;

class ProducerMsg {
public:
  virtual ~ProducerMsg();
};

class Producer {
public:
  typedef ProducerTopic Topic;
  typedef ProducerMsg Msg;
  Producer(BrokerOpt opt);
  ~Producer();
  void poll_while_outq();
  void poll();
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
  std::function<void(rd_kafka_message_t const *msg)> *on_delivery_ok = nullptr;
  void (*on_error)(Producer *, rd_kafka_resp_err_t) = nullptr;
  // Currently it's nice to have acces to these two for statistics:
  BrokerOpt opt;
  rd_kafka_t *rk = nullptr;

private:
  int id = 0;
};

class ProducerTopic {
public:
  ProducerTopic(Producer const &producer, std::string name);
  ~ProducerTopic();
  int produce(void *msg_data, int msg_size, void *opaque,
              bool print_err = false);
  // Currently it's nice to have access to these for statistics:
  Producer const &producer;
  rd_kafka_topic_t *rkt = nullptr;
  void do_copy();

private:
  std::string _name;
  bool _do_copy{ false };
};

} // namespace KafkaW
