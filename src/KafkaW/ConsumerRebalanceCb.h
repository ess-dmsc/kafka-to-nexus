#include <exception>
#include <librdkafka/rdkafkacpp.h>

class ConsumerRebalanceCb : public RdKafka::RebalanceCb {
public:
  void
  rebalance_cb(RdKafka::KafkaConsumer *consumer, RdKafka::ErrorCode err,
               std::vector<RdKafka::TopicPartition *> &partitions) override {
    std::string Topic = "unknown";
    if (partitions.size() > 0)
      Topic = partitions.at(0)->topic();
    throw std::runtime_error(
        fmt::format("CONSUMER: {}, TOPIC: {} ({} partitions), rebalancing "
                    "cannot be done, ERROR: {}",
                    consumer->name(), Topic, partitions.size(), err));
  }
};