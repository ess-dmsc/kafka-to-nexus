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
  typedef ProducerTopic Topic;
  typedef ProducerMessage Msg;
  explicit Producer(BrokerSettings ProducerBrokerSettings_);
  ~Producer() override;
  void poll() override;
  int outputQueueLength() override;
  RdKafka::Producer *getRdKafkaPtr() const override;
  // Currently it's nice to have access to these two for statistics:
  BrokerSettings ProducerBrokerSettings;
  std::atomic<uint64_t> TotalMessagesProduced{0};

private:
  std::unique_ptr<RdKafka::Producer> ProducerPtr = nullptr;
  int id = 0;
  ProducerDeliveryCb DeliveryCb{Stats};
  KafkaEventCb EventCb;
};
} // namespace KafkaW