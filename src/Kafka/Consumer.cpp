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

namespace {
/// Finds named topic in metadata. Throws if topic is not found.
///
/// \param Topic Name of the topic to look for.
/// \return The topic metadata object.
const RdKafka::TopicMetadata *
findTopic(std::string const &Topic, RdKafka::Metadata const &KafkaMetadata) {
  auto Topics = KafkaMetadata.topics();
  auto Iterator =
      std::find_if(Topics->cbegin(), Topics->cend(),
                   [Topic](const RdKafka::TopicMetadata *TopicMetadata) {
                     return TopicMetadata->topic() == Topic;
                   });
  if (Iterator == Topics->end()) {
    throw std::runtime_error(fmt::format("Topic {} does not exist", Topic));
  }
  return *Iterator;
}
} // namespace

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
    RdKafka::wait_destroyed(5000);
    Logger->debug("Consumer closed");
  }
}

void Consumer::addTopic(const std::string &Topic) {
  Logger->info("Consumer::add_topic  {}", Topic);
  std::vector<RdKafka::TopicPartition *> TopicPartitionsWithOffsets =
      queryWatermarkOffsets(Topic);
  assignToPartitions(Topic, TopicPartitionsWithOffsets);
}

std::vector<RdKafka::TopicPartition *>
Consumer::queryWatermarkOffsets(const std::string &Topic) {
  std::vector<RdKafka::TopicPartition *> TopicPartitionsWithOffsets;
  auto PartitionIDs = queryTopicPartitions(Topic);
  for (int PartitionID : PartitionIDs) {
    auto TopicPartition = RdKafka::TopicPartition::create(Topic, PartitionID);
    int64_t Low, High;
    auto ErrorCode = KafkaConsumer->query_watermark_offsets(
        Topic, PartitionID, &Low, &High,
        ConsumerBrokerSettings.MetadataTimeoutMS);
    if (ErrorCode != RdKafka::ERR_NO_ERROR) {
      Logger->error(
          "Unable to query watermark offsets for topic {} with error {} - {}",
          Topic, ErrorCode, RdKafka::err2str(ErrorCode));
      return {};
    }
    TopicPartition->set_offset(High);
    TopicPartitionsWithOffsets.push_back(TopicPartition);
  }
  return TopicPartitionsWithOffsets;
}

void Consumer::assignToPartitions(
    const std::string &Topic,
    const std::vector<RdKafka::TopicPartition *> &TopicPartitionsWithOffsets) {
  RdKafka::ErrorCode ErrorCode =
      KafkaConsumer->assign(TopicPartitionsWithOffsets);
  for_each(TopicPartitionsWithOffsets.cbegin(),
           TopicPartitionsWithOffsets.cend(),
           [this, &Topic](RdKafka::TopicPartition *Partition) {
             Logger->debug(
                 "Assigning partition to consumer: Topic {}, Partition {}, "
                 "Starting at offset {}",
                 Topic, Partition->partition(), Partition->offset());
             delete Partition;
           });
  if (ErrorCode != RdKafka::ERR_NO_ERROR) {
    Logger->error("Could not assign to {}", Topic);
    throw std::runtime_error(fmt::format(
        "Could not assign partitions of topic {}, RdKafka error: \"{}\"", Topic,
        err2str(ErrorCode)));
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
}

std::vector<int32_t> Consumer::queryTopicPartitions(const std::string &Topic) {
  std::unique_ptr<RdKafka::Metadata> KafkaMetadata = getMetadata();
  auto const matchedTopic = findTopic(Topic, *KafkaMetadata);
  std::vector<int32_t> TopicPartitionNumbers;
  const RdKafka::TopicMetadata::PartitionMetadataVector *PartitionMetadata =
      matchedTopic->partitions();
  for (const auto &Partition : *PartitionMetadata) {
    // cppcheck-suppress useStlAlgorithm
    TopicPartitionNumbers.push_back(Partition->id());
  }
  sort(TopicPartitionNumbers.begin(), TopicPartitionNumbers.end());
  return TopicPartitionNumbers;
}

/// Gets metadata from the Kafka broker, if unsuccessful then keeps trying
/// and logs a warning message every WarnOnNRetries attempts.
std::unique_ptr<RdKafka::Metadata> Consumer::getMetadata() {
  Logger->trace("Querying broker for Metadata");
  uint32_t LoopCounter = 0;
  uint32_t WarnOnNRetries = 10;
  std::unique_ptr<RdKafka::Metadata> KafkaMetadata = metadataCall();
  while (KafkaMetadata == nullptr) {
    if (LoopCounter == WarnOnNRetries) {
      Logger->warn("Cannot contact broker, retrying until connection is "
                   "established...");
      LoopCounter = 0;
    }
    KafkaMetadata = metadataCall();
    LoopCounter++;
  }
  Logger->trace("Successfully retrieved metadata from broker");
  return KafkaMetadata;
}

std::unique_ptr<RdKafka::Metadata> Consumer::metadataCall() {
  RdKafka::Metadata *MetadataPtr = nullptr;
  auto ErrorCode = KafkaConsumer->metadata(
      true, nullptr, &MetadataPtr, ConsumerBrokerSettings.MetadataTimeoutMS);
  switch (ErrorCode) {
  case RdKafka::ERR_NO_ERROR:
    return std::unique_ptr<RdKafka::Metadata>(MetadataPtr);
  case RdKafka::ERR__TRANSPORT:
    return nullptr;
  case RdKafka::ERR__TIMED_OUT:
    return nullptr;
  default:
    Logger->error("Error while retrieving metadata. Error code is: {}",
                  ErrorCode);
    throw MetadataException(
        fmt::format("Consumer::metadataCall() - error while retrieving "
                    "metadata. RdKafka errorcode: {}",
                    ErrorCode));
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
