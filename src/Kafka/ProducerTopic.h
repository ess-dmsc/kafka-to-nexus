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

class ProducerTopic {
public:
  ProducerTopic(std::shared_ptr<Producer> ProducerPtr, std::string TopicName);

  virtual ~ProducerTopic() = default;

  /// \brief Send a message to Kafka for publishing on this topic.
  ///
  /// \param MsgData The message to publish
  /// \return 0 if message is successfully passed to RdKafka to be published, 1
  /// otherwise
  virtual int produce(flatbuffers::DetachedBuffer const &MsgData);

  std::string name() const;

private:
  int produce(std::unique_ptr<Kafka::ProducerMessage> Msg);
  std::unique_ptr<RdKafka::Conf> ConfigPtr{
      RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)};
  std::shared_ptr<Producer> KafkaProducer;
  std::unique_ptr<RdKafka::Topic> RdKafkaTopic;
  std::string Name;
};
} // namespace Kafka
