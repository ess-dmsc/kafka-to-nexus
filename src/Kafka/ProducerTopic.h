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
#include <flatbuffers/flatbuffers.h>
#include <memory>
#include <string>

namespace Kafka {

class TopicCreationError : public std::runtime_error {
public:
  TopicCreationError() : std::runtime_error("Can not create Kafka topic") {}
};

class IProducerTopic {
public:
  IProducerTopic() = default;
  virtual ~IProducerTopic() = default;

  /// \brief Send a message to Kafka for publishing on this topic.
  ///
  /// \param MsgData The message to publish
  /// \return 0 if message is successfully passed to RdKafka to be published, 1
  /// otherwise
  int produce(flatbuffers::DetachedBuffer const &MsgData);
  [[nodiscard]] std::string name() const;

protected:
  std::string Name;

private:
  virtual int produce(std::unique_ptr<ProducerMessage> msg) = 0;
};

class ProducerTopic : public IProducerTopic {
public:
  ProducerTopic(std::shared_ptr<Producer> ProducerPtr, std::string TopicName);
  ~ProducerTopic() override = default;

private:
  int produce(std::unique_ptr<Kafka::ProducerMessage> Msg) override;
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
