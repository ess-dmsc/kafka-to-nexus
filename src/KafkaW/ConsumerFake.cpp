#include "ConsumerFake.h"

void KafkaW::ConsumerFake::addTopic(std::string const Topic) {}

bool KafkaW::ConsumerFake::topicPresent(const std::string &Topic) {
  return false;
}

std::vector<int32_t>
KafkaW::ConsumerFake::queryTopicPartitions(const std::string &TopicName) {
  return std::vector<int32_t>{1, 2, 3, 4, 5, 6};
}
