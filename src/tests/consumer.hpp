#include <memory>

namespace RdKafka {
class KafkaConsumer;
class Conf;
class TopicPartition;
} // namespace RdKafka

class Consumer : public ::testing::Test {
  std::unique_ptr<RdKafka::Conf> conf{nullptr};
  std::unique_ptr<RdKafka::KafkaConsumer> consumer{nullptr};
  std::vector<RdKafka::TopicPartition *> tp;
  std::unique_ptr<RdKafka::Metadata> metadata{nullptr};

  void push_topic_partition(const unsigned &);

protected:
  virtual void SetUp() {}

public:
  void create_config();
  void create_consumer();
  void create_topic_partition();
  void offsets_for_times();
  void create_metadata();
  void assign_topic_partition();
  void consume();

  static std::string broker;
  static std::string topic;
  static uint64_t timestamp;
};
