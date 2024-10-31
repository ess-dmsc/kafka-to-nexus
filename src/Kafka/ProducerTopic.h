// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Producer.h"
#include "logger.h"
#include <memory>
#include <string>

namespace flatbuffers {
class DetachedBuffer;
}

namespace Kafka {

class TopicCreationError : public std::runtime_error {
public:
  TopicCreationError() : std::runtime_error("Can not create Kafka topic") {}
};

class IProducerTopic {
public:
  IProducerTopic() = default;
  virtual ~IProducerTopic() = default;

  int produce(flatbuffers::DetachedBuffer const &MsgData);
  [[nodiscard]] std::string name() const;
  virtual int produce(std::unique_ptr<ProducerMessage> msg) = 0;

protected:
  std::string Name;
};

class ProducerTopic : public IProducerTopic {
public:
  ProducerTopic(std::shared_ptr<Producer> ProducerPtr,
                std::string const &TopicName);
  ~ProducerTopic() override = default;
  int produce(std::unique_ptr<Kafka::ProducerMessage> Msg) override;

private:
  std::unique_ptr<RdKafka::Conf> ConfigPtr{
      RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)};
  std::shared_ptr<Producer> KafkaProducer;
  std::unique_ptr<RdKafka::Topic> RdKafkaTopic;
};

class StubProducerTopic : public IProducerTopic {
public:
  explicit StubProducerTopic(std::string const &topic_name) {
    Name = topic_name;
  }
  ~StubProducerTopic() override = default;

  int produce([[maybe_unused]] std::unique_ptr<Kafka::ProducerMessage> message)
      override {
    messages.emplace_back(std::move(message));
    return 0;
  }

  std::vector<std::unique_ptr<Kafka::ProducerMessage>> messages;
};

} // namespace Kafka
