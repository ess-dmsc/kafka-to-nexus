// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Consumer.h"
#include "MetaDataQuery.h"
#include "MetadataException.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>

namespace Kafka {

Consumer::Consumer(std::unique_ptr<RdKafka::KafkaConsumer> RdConsumer,
                   std::unique_ptr<RdKafka::Conf> RdConf,
                   std::unique_ptr<KafkaEventCb> EventCb)
    : Conf(std::move(RdConf)), EventCallback(std::move(EventCb)),
      KafkaConsumer(std::move(RdConsumer)) {
  static std::atomic<int> ConsumerInstanceCount;
  id = ConsumerInstanceCount++;
}

Consumer::~Consumer() {
  if (KafkaConsumer != nullptr) {
    std::vector<std::string> Topics;
    auto ErrorCode = KafkaConsumer->subscription(Topics);
    if (ErrorCode == RdKafka::ERR_NO_ERROR) {
      LOG_DEBUG("Consumer consuming from topic(s) {} closed.", Topics);
    }
    KafkaConsumer->unassign();
    KafkaConsumer->unsubscribe();
    KafkaConsumer->close();
  }
}

void Consumer::addPartitionAtOffset(std::string const &Topic, int PartitionId,
                                    int64_t Offset) {
  LOG_INFO("Consumer::addPartitionAtOffset()  topic: {},  partitionId: {}, "
           "offset: {}",
           Topic, PartitionId, Offset);
  std::vector<RdKafka::TopicPartition *> Assignments;
  auto ErrorCode = KafkaConsumer->assignment(Assignments);
  if (ErrorCode != RdKafka::ERR_NO_ERROR) {
    LOG_ERROR("Could not assign to {}. Could not get current assignments.",
              Topic);
    throw std::runtime_error(fmt::format(
        R"(Could not assign topic-partition of topic {}. Could not get current assignments. RdKafka error: "{}")",
        Topic, err2str(ErrorCode)));
  }
  Assignments.emplace_back(
      RdKafka::TopicPartition::create(Topic, PartitionId, Offset));
  auto ReturnCode = KafkaConsumer->assign(Assignments);
  if (ReturnCode != RdKafka::ERR_NO_ERROR) {
    LOG_ERROR("Could not assign to {}", Topic);
    throw std::runtime_error(fmt::format(
        R"(Could not assign topic-partition of topic {}, RdKafka error: "{}")",
        Topic, err2str(ReturnCode)));
  }
  RdKafka::TopicPartition::destroy(Assignments);
}

void Consumer::addTopic(std::string const &Topic) {
  LOG_INFO("Consumer::addTopic()  topic: {}", Topic);
  std::vector<std::string> Topics;
  auto ErrorCode = KafkaConsumer->subscription(Topics);
  if (ErrorCode != RdKafka::ERR_NO_ERROR) {
    LOG_ERROR("Could not get current topic subscriptions.");
    throw std::runtime_error(fmt::format(
        R"(Could not get current topic subscriptions. RdKafka error: "{}")",
        err2str(ErrorCode)));
  }
  Topics.emplace_back(Topic);
  ErrorCode = KafkaConsumer->subscribe(Topics);
  if (ErrorCode != RdKafka::ERR_NO_ERROR) {
    LOG_ERROR(R"(Unable to add topic "{}" to list of subscribed topics.)",
              Topic);
    throw std::runtime_error(fmt::format(
        R"(Unable to add topic "{}" to list of subscribed topics. RdKafka
        error: "{}")",
        Topic, err2str(ErrorCode)));
  }
}

void Consumer::assignAllPartitions(std::string const &Topic,
                                   time_point const &StartTimestamp) {
  LOG_INFO("Consumer::assignAllPartitions()  Topic: {} StartTimestamp: {}",
           Topic, StartTimestamp);
  // Obtain partitions
  RdKafka::Metadata *MetadataPtr{nullptr};
  const RdKafka::TopicMetadata *TopicMetadata =
      getTopicMetadata(Topic, MetadataPtr);
  if (TopicMetadata == nullptr) {
    throw std::runtime_error(fmt::format(
        R"(Could not assign partitions in topic "{}" for the provided timestamp "{}". Topic metadata not found)",
        Topic, StartTimestamp));
  }
  std::vector<RdKafka::TopicPartition *> TopicPartitions;
  for (auto const &Partition : *TopicMetadata->partitions()) {
    LOG_DEBUG(
        R"(Obtaining offset for topic "{}" partition "{}" for the provided timestamp "{}")",
        Topic, Partition->id(), StartTimestamp);
    TopicPartitions.push_back(RdKafka::TopicPartition::create(
        Topic, Partition->id(), toMilliSeconds(StartTimestamp)));
  }
  // Obtain offsets
  auto ErrorCode = KafkaConsumer->offsetsForTimes(
      TopicPartitions,
      toMilliSeconds(ConsumerBrokerSettings.MaxMetadataTimeout));
  if (ErrorCode != RdKafka::ERR_NO_ERROR) {
    LOG_ERROR(
        R"(Could not get offsets in topic "{}" for the provided timestamp "{}". RdKafka error: "{}")",
        Topic, StartTimestamp, err2str(ErrorCode));
    throw std::runtime_error(fmt::format(
        R"(Could not get offsets in topic "{}" for the provided timestamp "{}". RdKafka error: "{}")",
        Topic, StartTimestamp, err2str(ErrorCode)));
  }
  // Assign partitions at offset
  ErrorCode = KafkaConsumer->assign(TopicPartitions);
  if (ErrorCode != RdKafka::ERR_NO_ERROR) {
    LOG_ERROR(R"(Could not assign topic-partitions. RdKafka error: "{}")",
              err2str(ErrorCode));
    throw std::runtime_error(
        fmt::format(R"(Could not assign topic-partitions. RdKafka error: "{}")",
                    err2str(ErrorCode)));
  }
  RdKafka::TopicPartition::destroy(TopicPartitions);
  delete MetadataPtr;
}

const RdKafka::TopicMetadata *
Consumer::getTopicMetadata(const std::string &Topic,
                           RdKafka::Metadata *MetadataPtr) {
  std::string ErrorStr;
  auto TopicObj = std::unique_ptr<RdKafka::Topic>(
      RdKafka::Topic::create(KafkaConsumer.get(), Topic, nullptr, ErrorStr));
  auto ReturnCode = KafkaConsumer->metadata(
      true, TopicObj.get(), &MetadataPtr,
      toMilliSeconds(ConsumerBrokerSettings.MaxMetadataTimeout));
  if (ReturnCode != RdKafka::ERR_NO_ERROR) {
    throw MetadataException(fmt::format(
        R"(Failed to query broker for available partitions on topic "{}". Error was: {})",
        Topic, RdKafka::err2str(ReturnCode)));
  }

  const auto *Topics = MetadataPtr->topics();
  auto Iterator =
      std::find_if(Topics->cbegin(), Topics->cend(),
                   [&Topic](const RdKafka::TopicMetadata *TopicMetadata) {
                     return TopicMetadata->topic() == Topic;
                   });
  if (Iterator == Topics->end()) {
    throw MetadataException(
        fmt::format(R"(Topic "{}" not listed by broker.)", Topic));
  }
  return *Iterator;
}

std::pair<PollStatus, FileWriter::Msg> Consumer::poll() {
  auto KafkaMsg = std::unique_ptr<RdKafka::Message>(KafkaConsumer->consume(
      toMilliSeconds(ConsumerBrokerSettings.PollTimeout)));
  switch (KafkaMsg->err()) {
  case RdKafka::ERR_NO_ERROR: {
    // TODO: add topic name to metadata
    auto MetaData = FileWriter::MessageMetaData{
        std::chrono::milliseconds(KafkaMsg->timestamp().timestamp),
        KafkaMsg->timestamp().type, KafkaMsg->offset(), KafkaMsg->partition(),
        KafkaMsg->topic_name()};
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
    // No more messages on the partition
    return {PollStatus::EndOfPartition, FileWriter::Msg()};
  default:
    // Everything else is an error
    return {PollStatus::Error, FileWriter::Msg()};
  }
}
} // namespace Kafka
