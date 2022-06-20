// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "BrokerSettings.h"
#include "KafkaEventCb.h"
#include "ProducerDeliveryCb.h"
#include "ProducerMessage.h"
#include "ProducerStats.h"
#include <atomic>

namespace Kafka {

class Producer {
public:
  /// The constructor.
  ///
  /// \param Settings_ The BrokerSettings.
  explicit Producer(BrokerSettings const &Settings);
  virtual ~Producer();

  /// Polls Kafka for events.
  void poll();

  /// Gets the number of messages not send.
  ///
  /// \return The number of messages.
  int outputQueueLength();

  RdKafka::Producer *getRdKafkaPtr() const;

  /// Send a message to Kafka.
  ///
  /// \param Topic The topic to publish to.
  /// \param Partition The topic partition to publish to.
  /// \param MessageFlags
  /// \param Payload The actual message data.
  /// \param PayloadSize The size of the payload.
  /// \param Key The message's key.
  /// \param KeySize The size of the key.
  /// \param OpaqueMessage Points to the whole message.
  /// \return The Kafka RESP error code.
  RdKafka::ErrorCode produce(RdKafka::Topic *Topic, int32_t Partition,
                             int MessageFlags, void *Payload,
                             size_t PayloadSize, const void *Key,
                             size_t KeySize, void *OpaqueMessage);
  BrokerSettings const ProducerBrokerSettings;
  std::atomic<uint64_t> TotalMessagesProduced{0};
  ProducerStats Stats;

protected:
  int ProducerID = 0;

private:
  void setConf(std::string &ErrorString);
  std::unique_ptr<RdKafka::Conf> Conf = std::unique_ptr<RdKafka::Conf>(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  ProducerDeliveryCb DeliveryCb{Stats};
  KafkaEventCb EventCb;

protected:
  std::unique_ptr<RdKafka::Producer> ProducerPtr;
};
} // namespace Kafka
