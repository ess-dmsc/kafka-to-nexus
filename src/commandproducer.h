#pragma once
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <string>

namespace FileWriter {

class BrokerOpt {
public:
  std::string address = "localhost:9092";
  std::string topic = "ess-file-writer.command";
};

class Producer {
public:
  Producer(BrokerOpt opt);
  ~Producer();
  // void produce(void const * msg_data, int msg_size);
  void poll_outq();
  void poll();
  static void cb_delivered(rd_kafka_t *rk, rd_kafka_message_t const *msg,
                           void *opaque);
  static void cb_error(rd_kafka_t *rk, int err_i, char const *reason,
                       void *opaque);
  static int cb_stats(rd_kafka_t *rk, char *json, size_t json_len,
                      void *opaque);
  static void cb_log(rd_kafka_t const *rk, int level, char const *fac,
                     char const *buf);
  rd_kafka_t *rd_kafka_ptr() const;
  std::function<void(rd_kafka_message_t const *msg)> *on_delivery = nullptr;

private:
  BrokerOpt opt;
  int poll_timeout_ms = 10;
  rd_kafka_t *rk = nullptr;
  // rd_kafka_topic_t * rkt = nullptr;
  rd_kafka_topic_partition_list_t *plist = nullptr;
};

class ProducerTopic {
public:
  ProducerTopic(Producer const &producer, std::string name);
  ~ProducerTopic();
  void produce(void *msg_data, int msg_size);

private:
  Producer const &producer;
  rd_kafka_topic_t *rkt = nullptr;
  std::string _name;
};

} // namespace FileWriter
