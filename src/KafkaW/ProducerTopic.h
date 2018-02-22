#pragma once

#include "Msg.h"
#include "Producer.h"
#include "TopicSettings.h"
#include "logger.h"
#include <memory>
#include <string>

namespace KafkaW {

class ProducerTopic {
public:
  ProducerTopic(ProducerTopic &&);
  ProducerTopic(std::shared_ptr<Producer> Producer_, std::string Name_);
  ~ProducerTopic();
  int produce(uchar *msg_data, size_t msg_size, bool print_err = false);
  int produce(std::unique_ptr<Producer::Msg> &msg);
  // Currently it's nice to have access to these for statistics:
  std::shared_ptr<Producer> Producer_;
  rd_kafka_topic_t *rkt = nullptr;
  void do_copy();

private:
  std::string Name;
  bool DoCopyMsg{false};
};
}
