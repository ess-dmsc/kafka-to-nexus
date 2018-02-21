#pragma once

#include "BrokerSettings.h"
#include "Consumer.h"
#include "Msg.h"
#include "Producer.h"
#include "logger.h"
#include <atomic>
#include <functional>
#include <librdkafka/rdkafka.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#if HAVE_KAFKAW_INSPECT
#include "KafkaW-inspect.h"
#endif

namespace KafkaW {

class TopicOpt {
public:
  TopicOpt();
  void apply(rd_kafka_topic_conf_t *conf);
  std::map<std::string, int> conf_ints;
  std::map<std::string, std::string> conf_strings;
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
