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

class ProducerInterface {
public:
  ProducerInterface() = default;
  virtual ~ProducerInterface() = default;
  virtual void poll() = 0;
  virtual int outputQueueLength() = 0;
  virtual RdKafka::Producer *getRdKafkaPtr() const = 0;
  ProducerStats Stats;
};

class Producer : public ProducerInterface {
public:
  /// The constructor.
  ///
  /// \param Settings_ The BrokerSettings.
  explicit Producer(BrokerSettings Settings);
  ~Producer() override;

  /// Polls Kafka for events.
  void poll() override;

  /// Gets the number of messages not send.
  ///
  /// \return The number of messages.
  int outputQueueLength() override;

  RdKafka::Producer *getRdKafkaPtr() const override;

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
  BrokerSettings ProducerBrokerSettings;
  std::atomic<uint64_t> TotalMessagesProduced{0};

protected:
  int ProducerID = 0;
  std::unique_ptr<RdKafka::Handle> ProducerPtr = nullptr;

private:
  std::unique_ptr<RdKafka::Conf> Conf;
  ProducerDeliveryCb DeliveryCb{Stats};
  KafkaEventCb EventCb;
};
} // namespace KafkaW
