#pragma once

#include "BrokerSettings.h"
#include "ConsumerEventCb.h"
#include "ConsumerMessage.h"
#include "ConsumerRebalanceCb.h"
#include <chrono>
#include <functional>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <memory>

namespace FileWriter {
class Msg;
}

namespace KafkaW {

class ConsumerInterface {
public:
  ConsumerInterface() = default;
  virtual ~ConsumerInterface() = default;
  virtual void addTopic(std::string const Topic) = 0;
  virtual void
  addTopicAtTimestamp(std::string const Topic,
                      std::chrono::milliseconds const StartTime) = 0;
  virtual void poll(PollStatus &Status, FileWriter::Msg &Message) = 0;
  virtual void dumpCurrentSubscription() = 0;
  virtual bool topicPresent(const std::string &Topic) = 0;
  virtual std::vector<int32_t>
  queryTopicPartitions(const std::string &TopicName) = 0;
};

class Consumer : public ConsumerInterface {
public:
  explicit Consumer(const BrokerSettings &opt);
  Consumer(Consumer &&) = delete;
  Consumer(Consumer const &) = delete;
  ~Consumer() override;
  void addTopic(std::string const Topic) override;
  void addTopicAtTimestamp(std::string const Topic,
                           std::chrono::milliseconds const StartTime) override;
  void dumpCurrentSubscription() override;
  bool topicPresent(const std::string &Topic) override;
  std::vector<int32_t>
  queryTopicPartitions(const std::string &TopicName) override;
  void poll(PollStatus &Status, FileWriter::Msg &Message) override;
  std::function<void(rd_kafka_topic_partition_list_t *plist)>
      on_rebalance_assign;
  std::function<void(rd_kafka_topic_partition_list_t *plist)>
      on_rebalance_start;
  rd_kafka_t *RdKafka = nullptr;

private:
  //    const RdKafka::TopicMetadata::PartitionMetadataVector *
  //    getTopicPartitionsVector(const std::string &Topic);
  BrokerSettings ConsumerBrokerSettings;
  std::unique_ptr<RdKafka::Metadata> queryMetadata();
  std::shared_ptr<RdKafka::KafkaConsumer> KafkaConsumer;

  rd_kafka_topic_partition_list_t *PartitionList = nullptr;
  int id = 0;
  ConsumerEventCb EventCallback;
  ConsumerRebalanceCb RebalanceCallback;
  //  void commitOffsets() const;
};
} // namespace KafkaW
