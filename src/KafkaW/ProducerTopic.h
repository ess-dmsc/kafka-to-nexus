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
  /// NB this copies the provided data - so use only for low volume publishing
  /// \param MsgData Pointer to the data to publish
  /// \param MsgSize Size of the data to publish
  /// \return 0 if message is successfully passed to RdKafka to be published, 1
  /// otherwise
  int produce(const std::string &MsgData);
  int produce(std::unique_ptr<KafkaW::ProducerMessage> &Msg);
  std::string name() const;

private:
  std::unique_ptr<RdKafka::Conf> Config;
  std::shared_ptr<Producer> KafkaProducer;
  std::unique_ptr<RdKafka::Topic> RdKafkaTopic;
  std::string Name;
};
}
