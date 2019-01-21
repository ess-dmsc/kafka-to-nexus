#pragma once

#include "Producer.h"
#include "ProducerMessage.h"
#include "logger.h"
#include <memory>
#include <string>

namespace KafkaW {

class TopicCreationError : public std::runtime_error {
public:
  TopicCreationError() : std::runtime_error("Can not create Kafka topic") {}
};

class ProducerTopic {
public:
  ProducerTopic(ProducerTopic &&) noexcept;

  ProducerTopic(std::shared_ptr<Producer> ProducerPtr, std::string TopicName);

  ~ProducerTopic() = default;

  int produce(unsigned char *MsgData, size_t MsgSize);

  int produce(std::unique_ptr<KafkaW::ProducerMessage> &Msg);

  // Currently it's nice to have access to these for statistics:
  std::shared_ptr<Producer> KafkaProducer;
  RdKafka::Topic *RdKafkaTopic = nullptr;

  void enableCopy();

  std::string name() const;

private:
  std::string Name;
  bool DoCopyMsg{false};
};
}