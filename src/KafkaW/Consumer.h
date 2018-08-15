#pragma once

#include "BrokerSettings.h"
#include "Msg.h"
#include "PollStatus.h"
#include <chrono>
#include <functional>
#include <librdkafka/rdkafka.h>

namespace KafkaW {

class ConsumerBase {
public:
  ConsumerBase() = default;
  virtual ~ConsumerBase() = default;
  virtual void addTopic(std::string Topic,
                        const std::chrono::milliseconds &StartTime =
                            std::chrono::milliseconds{0}) = 0;
  virtual PollStatus poll() = 0;
  virtual void dumpCurrentSubscription() = 0;
  virtual bool topicPresent(const std::string &Topic) = 0;
  virtual int32_t queryNumberOfPartitions(const std::string &TopicName) = 0;
};

class Consumer : public ConsumerBase {
public:
  Consumer(BrokerSettings opt);
  Consumer(Consumer &&) = delete;
  Consumer(Consumer const &) = delete;
  virtual ~Consumer();
  void init();
  void addTopic(std::string Topic, const std::chrono::milliseconds &StartTime =
                                       std::chrono::milliseconds{0}) override;
  void dumpCurrentSubscription() override;
  bool topicPresent(const std::string &Topic) override;
  int32_t queryNumberOfPartitions(const std::string &TopicName) override;
  PollStatus poll() override;
  std::function<void(rd_kafka_topic_partition_list_t *plist)>
      on_rebalance_assign;
  std::function<void(rd_kafka_topic_partition_list_t *plist)>
      on_rebalance_start;
  rd_kafka_t *RdKafka = nullptr;

private:
  BrokerSettings ConsumerBrokerSettings;
  static void cb_log(rd_kafka_t const *rk, int level, char const *fac,
                     char const *buf);
  static int cb_stats(rd_kafka_t *rk, char *json, size_t json_size,
                      void *opaque);
  static void cb_error(rd_kafka_t *rk, int err_i, char const *reason,
                       void *opaque);
  static void cb_rebalance(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                           rd_kafka_topic_partition_list_t *plist,
                           void *opaque);
  rd_kafka_topic_partition_list_t *PartitionList = nullptr;
  int id = 0;

  void commitOffsets() const;
};
} // namespace KafkaW
