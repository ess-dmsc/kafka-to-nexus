// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Consumer.h"
#include "MetadataException.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>

namespace Kafka {

static std::atomic<int> ConsumerInstanceCount;

Consumer::Consumer(std::unique_ptr<RdKafka::KafkaConsumer> RdConsumer,
                   std::unique_ptr<RdKafka::Conf> RdConf,
                   std::unique_ptr<KafkaEventCb> EventCb)
    : KafkaConsumer(std::move(RdConsumer)), Conf(std::move(RdConf)),
      EventCallback(std::move(EventCb)) {
  id = ConsumerInstanceCount++;
}

Consumer::~Consumer() {
  Logger->debug("~Consumer()");
  if (KafkaConsumer != nullptr) {
    KafkaConsumer->close();
    Logger->debug("Consumer closed");
  }
}

void Consumer::addPartitionAtOffset(std::string const &Topic, int PartitionId,
                                    int64_t Offset) {
  Logger->info("Consumer::addPartitionAtOffset()  topic: {},  partitionId: {}, "
               "offset: {}",
               Topic, PartitionId, Offset);
  std::vector<RdKafka::TopicPartition *> Assignments;
  auto ErrorCode = KafkaConsumer->assignment(Assignments);
  if (ErrorCode != RdKafka::ERR_NO_ERROR) {
    Logger->error("Could not assign to {}. Could not get current assignments.",
                  Topic);
    throw std::runtime_error(
        fmt::format("Could not assign topic-partition of topic {}. Could not "
                    "get current assignments. RdKafka error: \"{}\"",
                    Topic, err2str(ErrorCode)));
  }
  Assignments.emplace_back(
      RdKafka::TopicPartition::create(Topic, PartitionId, Offset));
  auto ReturnCode = KafkaConsumer->assign(Assignments);
  if (ReturnCode != RdKafka::ERR_NO_ERROR) {
    Logger->error("Could not assign to {}", Topic);
    throw std::runtime_error(fmt::format(
        "Could not assign topic-partition of topic {}, RdKafka error: \"{}\"",
        Topic, err2str(ReturnCode)));
  }
  for (auto Ptr : Assignments) {
    delete Ptr;
  }
}

void Consumer::addTopic(std::string const &Topic) {
  Logger->info("Consumer::addTopic()  topic: {}", Topic);
  std::vector<std::string> Topics;
  auto ErrorCode = KafkaConsumer->subscription(Topics);
  if (ErrorCode != RdKafka::ERR_NO_ERROR) {
    Logger->error("Could not get current topic subscriptions.");
    throw std::runtime_error(fmt::format(
        "Could not get current topic subscriptions. RdKafka error: \"{}\"",
        err2str(ErrorCode)));
  }
  Topics.emplace_back(Topic);
  ErrorCode = KafkaConsumer->subscribe(Topics);
  if (ErrorCode != RdKafka::ERR_NO_ERROR) {
    Logger->error("Unable to add topic \"{}\" to list of subscribed topics.",
                  Topic);
    throw std::runtime_error(
        fmt::format("Unable to add topic \"{}\" to list of subscribed topics. "
                    "RdKafka error: \"{}\"",
                    Topic, err2str(ErrorCode)));
  }
}

std::pair<PollStatus, FileWriter::Msg> Consumer::poll() {
  auto KafkaMsg = std::unique_ptr<RdKafka::Message>(
      KafkaConsumer->consume(ConsumerBrokerSettings.PollTimeoutMS));
  switch (KafkaMsg->err()) {
  case RdKafka::ERR_NO_ERROR: {
    auto MetaData = FileWriter::MessageMetaData{
        std::chrono::milliseconds(KafkaMsg->timestamp().timestamp),
        KafkaMsg->timestamp().type, KafkaMsg->offset(), KafkaMsg->partition()};
    auto RetMsg =
        FileWriter::Msg(reinterpret_cast<const char *>(KafkaMsg->payload()),
                        KafkaMsg->len(), MetaData);
    return {PollStatus::Message, std::move(RetMsg)};
  }
  case RdKafka::ERR__TIMED_OUT:
    // No message or event within time out - this is usually normal (see
    // librdkafka docs)
    return {PollStatus::TimedOut, FileWriter::Msg()};
  case RdKafka::ERR__PARTITION_EOF:
    return {PollStatus::EndOfPartition, FileWriter::Msg()};
  default:
    // Everything else is an error
    return {PollStatus::Error, FileWriter::Msg()};
  }
}
} // namespace Kafka
