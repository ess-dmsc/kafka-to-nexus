#pragma once

#include "BrokerSettings.h"
#include "ConsumerEventCb.h"
#include "ConsumerMessage.h"
#include "ConsumerRebalanceCb.h"
#include "KafkaW.h"
#include <chrono>
#include <functional>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <memory>

namespace KafkaW {
class ConsumerFake : public ConsumerInterface {
public:
  explicit ConsumerFake(const BrokerSettings &opt):ConsumerBrokerSettings(opt){};
  ~ConsumerFake() override;
  void addTopic(std::string const Topic) override;
  void addTopicAtTimestamp(std::string const Topic,
                           std::chrono::milliseconds const StartTime) override;
  void dumpCurrentSubscription() override;
  bool topicPresent(const std::string &Topic) override;
  std::vector<int32_t>
  queryTopicPartitions(const std::string &TopicName) override;
  std::unique_ptr<ConsumerMessage> poll() override;

private:
  BrokerSettings ConsumerBrokerSettings;
  //  void commitOffsets() const;
};
}