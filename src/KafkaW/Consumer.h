#pragma once

#include "BrokerSettings.h"
#include "ConsumerMessage.h"
#include <chrono>
#include <functional>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include "ConsumerEventCb.h"
#include "ConsumerRebalanceCb.h"

namespace KafkaW {

class ConsumerInterface {
public:
  ConsumerInterface() = default;
  virtual ~ConsumerInterface() = default;
  virtual void addTopic(std::string const Topic) = 0;
  virtual void
  addTopicAtTimestamp(std::string const Topic,
                      std::chrono::milliseconds const StartTime) = 0;
  virtual std::unique_ptr<ConsumerMessage> poll() = 0;
  virtual void dumpCurrentSubscription() = 0;
  virtual bool topicPresent(const std::string &Topic) = 0;
  virtual int32_t queryNumberOfPartitions(const std::string &TopicName) = 0;
};

class Consumer : public ConsumerInterface {
public:
  explicit Consumer(const BrokerSettings& opt);
  Consumer(Consumer &&) = delete;
  Consumer(Consumer const &) = delete;
  ~Consumer() override;
  void addTopic(std::string const Topic) override;
  void addTopicAtTimestamp(std::string const Topic,
                           std::chrono::milliseconds const StartTime) override;
  void dumpCurrentSubscription() override;
  bool topicPresent(const std::string &Topic) override;
  int32_t queryNumberOfPartitions(const std::string &TopicName) override;
  std::unique_ptr<ConsumerMessage> poll() override;
  std::function<void(rd_kafka_topic_partition_list_t *plist)>
      on_rebalance_assign;
  std::function<void(rd_kafka_topic_partition_list_t *plist)>
      on_rebalance_start;
  rd_kafka_t *RdKafka = nullptr;

private:
  BrokerSettings ConsumerBrokerSettings;

  std::shared_ptr<RdKafka::KafkaConsumer> KafkaConsumer;

  rd_kafka_topic_partition_list_t *PartitionList = nullptr;
  int id = 0;
    ConsumerEventCb EventCallback;
    ConsumerRebalanceCb RebalanceCallback;
  void commitOffsets() const;
};
} // namespace KafkaW
