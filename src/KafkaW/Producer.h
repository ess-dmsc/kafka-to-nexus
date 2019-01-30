#pragma once

#include "BrokerSettings.h"
#include "KafkaEventCb.h"
#include "ProducerDeliveryCb.h"
#include "ProducerMessage.h"
#include "ProducerStats.h"
#include <atomic>
#include <functional>

namespace KafkaW {

class ProducerTopic;

class Producer {
public:
  /// The constructor.
  ///
  /// \param Settings_ The BrokerSettings.
  explicit Producer(BrokerSettings &Settings);
  virtual ~Producer();

  /// Polls Kafka for events.
  void poll();

  /// Gets the number of messages not send.
  ///
  /// \return The number of messages.
  virtual int outputQueueLength();

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
  BrokerSettings &ProducerBrokerSettings;
  std::atomic<uint64_t> TotalMessagesProduced{0};
  ProducerStats Stats;

protected:
  int ProducerID = 0;
  std::unique_ptr<RdKafka::Producer> ProducerPtr = nullptr;

private:
  std::unique_ptr<RdKafka::Conf> Conf;
  ProducerDeliveryCb DeliveryCb{Stats};
  KafkaEventCb EventCb;
};
} // namespace KafkaW
