#pragma once

#include "ConsumerMessage.h"
#include "Producer.h"
#include "TopicSettings.h"
#include "logger.h"
#include <memory>
#include <string>

namespace KafkaW {

class TopicCreationError : public std::runtime_error {
public:
  TopicCreationError() : std::runtime_error("Can not create Kafka topic") {}
};

enum ProducerTopicError {
  RDKAFKATOPIC_NOT_INITIALIZED,
};

class ProducerTopic {
public:
  ProducerTopic(ProducerTopic &&);
  ProducerTopic(std::shared_ptr<Producer> Pointer, std::string Topic);
  ~ProducerTopic();
  int produce(uchar *MsgData, size_t MsgSize, bool PrintError = false);
  int produce(std::unique_ptr<Producer::Msg> &Msg);
  // Currently it's nice to have access to these for statistics:
  std::shared_ptr<Producer> ProducerPtr;
  rd_kafka_topic_t *RdKafkaTopic = nullptr;
  void enableCopy();

private:
  std::string TopicName;
  bool DoCopyMsg{false};
};
}
