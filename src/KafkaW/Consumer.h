#pragma once

#include "BrokerSettings.h"
#include "ConsumerRebalanceCb.h"
#include "KafkaEventCb.h"
#include "PollStatus.h"
#include <chrono>
#include <functional>
#include <librdkafka/rdkafkacpp.h>
#include <memory>

namespace FileWriter {
struct Msg;
}

namespace KafkaW {

class ConsumerInterface {
public:
  ConsumerInterface() = default;
  virtual ~ConsumerInterface() = default;
  virtual void addTopic(std::string const &Topic) = 0;
  virtual void
  addTopicAtTimestamp(std::string const &Topic,
                      std::chrono::milliseconds const StartTime) = 0;
  virtual std::unique_ptr<std::pair<PollStatus, FileWriter::Msg>> poll() = 0;
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
  void addTopic(std::string const &Topic) override;
  void addTopicAtTimestamp(std::string const &Topic,
                           std::chrono::milliseconds const StartTime) override;
  void dumpCurrentSubscription() override;
  bool topicPresent(const std::string &Topic) override;
  std::vector<int32_t>
  queryTopicPartitions(const std::string &TopicName) override;
  std::unique_ptr<std::pair<PollStatus, FileWriter::Msg>> poll() override;

protected:
  std::unique_ptr<RdKafka::KafkaConsumer> KafkaConsumer;

private:
  std::unique_ptr<RdKafka::Conf> Conf;
  BrokerSettings ConsumerBrokerSettings;
  void queryMetadata();
  std::unique_ptr<RdKafka::Metadata> KafkaMetadata;
  int id = 0;
  KafkaEventCb EventCallback;
  ConsumerRebalanceCb RebalanceCallback;
};
} // namespace KafkaW