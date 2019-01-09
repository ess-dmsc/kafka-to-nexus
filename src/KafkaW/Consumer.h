#pragma once

#include "BrokerSettings.h"
#include "ConsumerMessage.h"
#include "ConsumerSettings.h"
#include <chrono>
#include <functional>
#include <librdkafka/rdkafka.h>
#include <memory>

namespace KafkaW {

class ConsumerInterface {
public:
  ConsumerInterface() = default;
  virtual ~ConsumerInterface() = default;
  virtual void addTopic(std::string const &Topic) = 0;
  virtual void
  addTopicAtTimestamp(std::string const &Topic,
                      std::chrono::milliseconds StartTime) = 0;
  virtual std::unique_ptr<ConsumerMessage> poll() = 0;
  virtual bool topicPresent(const std::string &Topic) = 0;
  virtual int32_t queryNumberOfPartitions(const std::string &TopicName) = 0;
};

class Consumer : public ConsumerInterface {
public:
  explicit Consumer(BrokerSettings BrokerOpt,
                    ConsumerSettings ConsumerOpt = ConsumerSettings());
  explicit Consumer(Consumer &&) = delete;
  explicit Consumer(Consumer const &) = delete;
  ~Consumer() override;
  void init();
  void addTopic(std::string const &Topic) override;
  void addTopicAtTimestamp(std::string const &Topic,
                           std::chrono::milliseconds StartTime) override;
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
  ConsumerSettings Settings;
  static void cb_log(rd_kafka_t const *rk, int level, char const *fac,
                     char const *buf);
  static int cb_stats(rd_kafka_t *rk, char *json, size_t json_size,
                      void *opaque);
  static void cb_error(rd_kafka_t *rk, int err_i, char const *msg,
                       void *opaque);
  static void cb_rebalance(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                           rd_kafka_topic_partition_list_t *plist,
                           void *opaque);
  rd_kafka_topic_partition_list_t *PartitionList = nullptr;
  int id = 0;

  void commitOffsets() const;
};
} // namespace KafkaW
