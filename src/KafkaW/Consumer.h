#pragma once

#include "BrokerSettings.h"
#include "ConsumerEventCb.h"
#include "ConsumerRebalanceCb.h"
#include "PollStatus.h"
#include <chrono>
#include <functional>
#include <librdkafka/rdkafka.h>
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
  virtual void addTopic(std::string const Topic) = 0;
  virtual void
  addTopicAtTimestamp(std::string const Topic,
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
  void addTopic(std::string const Topic) override;
  void addTopicAtTimestamp(std::string const Topic,
                           std::chrono::milliseconds const StartTime) override;
  void dumpCurrentSubscription() override;
  bool topicPresent(const std::string &Topic) override;
  std::vector<int32_t>
  queryTopicPartitions(const std::string &TopicName) override;
  std::unique_ptr<std::pair<PollStatus, FileWriter::Msg>> poll() override;

protected:
  std::shared_ptr<RdKafka::KafkaConsumer> KafkaConsumer;

private:
  BrokerSettings ConsumerBrokerSettings;
  std::unique_ptr<RdKafka::Metadata> queryMetadata();

  int id = 0;
  ConsumerEventCb EventCallback;
  ConsumerRebalanceCb RebalanceCallback;
};
} // namespace KafkaW
