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
  ProducerTopic(std::shared_ptr<Producer> ProducerPtr, std::string TopicName);
  ~ProducerTopic() = default;
  int produce(unsigned char *MsgData, size_t MsgSize);
  int produce(std::unique_ptr<KafkaW::ProducerMessage> &Msg);
  void enableCopy();
  std::string name() const;

private:
  std::unique_ptr<RdKafka::Conf> Config;
  std::shared_ptr<Producer> KafkaProducer;
  std::unique_ptr<RdKafka::Topic> RdKafkaTopic;
  std::string Name;

  bool DoCopyMsg{false};
};
}
