#include <memory>

namespace RdKafka {
class Conf;
class Producer;
class TopicPartition;
} // namespace RdKafka

class Producer : public ::testing::Test {
  std::unique_ptr<RdKafka::Conf> conf{nullptr};
  std::unique_ptr<RdKafka::Producer> producer{nullptr};
  std::vector<RdKafka::TopicPartition *> tp;

  void create_config();
  void create_producer();

protected:
  virtual void SetUp();

public:
  static std::string broker;
  void produce(const std::string &,
               const int32_t &partition = RdKafka::Topic::PARTITION_UA);
};
