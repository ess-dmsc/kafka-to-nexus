#pragma once
#include <exception>
#include <fmt/format.h>
#include <librdkafka/rdkafkacpp.h>

/// This will throw an error if a rebalance ever occurs - this should never
/// happen as we should not be using assign instead of subscribe to consume from
/// a certain offset.
class ConsumerRebalanceCb : public RdKafka::RebalanceCb {
public:
  void
  rebalance_cb(RdKafka::KafkaConsumer *consumer, RdKafka::ErrorCode err,
               std::vector<RdKafka::TopicPartition *> &partitions) override {
    std::string Topic = "unknown";
    if (!partitions.empty())
      Topic = partitions.at(0)->topic();
    throw std::runtime_error(
        fmt::format("CONSUMER: {}, TOPIC: {} ({} partitions), rebalancing "
                    "cannot be done, ERROR: {}",
                    consumer->name(), Topic, partitions.size(), err));
  }
};
