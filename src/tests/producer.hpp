#include <memory>

namespace RdKafka {
class Conf;
class Producer;
class TopicPartition;
} // namespace RdKafka

class Producer {
  std::unique_ptr<RdKafka::Conf> conf{nullptr};
  std::unique_ptr<RdKafka::Producer> producer{nullptr};
  std::vector<RdKafka::TopicPartition *> tp;

  void create_config();
  void create_producer();

protected:
public:
  static std::string broker;
  static std::string topic;
  void SetUp();
  void produce(const std::string &t = topic,
               const int32_t &partition = RdKafka::Topic::PARTITION_UA);
  int poll(const int &);
};
