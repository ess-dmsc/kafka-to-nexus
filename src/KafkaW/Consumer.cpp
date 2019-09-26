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
#include "Msg.h"
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
findTopic(const std::string &Topic,
          const std::shared_ptr<RdKafka::Metadata> &KafkaMetadata) {
  auto Topics = KafkaMetadata->topics();
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
}

namespace KafkaW {

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
  CurrentTopic = Topic;
  CurrentNumberOfPartitions = TopicPartitionsWithOffsets.size();
}

std::vector<int64_t> Consumer::getCurrentOffsets(std::string const &Topic) {
  auto NumberOfPartitions = queryTopicPartitions(Topic).size();
  std::vector<RdKafka::TopicPartition *> TopicPartitions;
  for (uint64_t i = 0; i < NumberOfPartitions; i++) {
    auto TopicPartition = RdKafka::TopicPartition::create(Topic, i);
    TopicPartitions.push_back(TopicPartition);
  }

  auto Error = KafkaConsumer->position(TopicPartitions);
  if (Error != RdKafka::ErrorCode::ERR_NO_ERROR) {
    Logger->error("Kafka error while getting current offsets for topic {}: {}",
                  Topic, Error);
    throw std::runtime_error(fmt::format(
        "Kafka error while getting offsets for topic {}: {}", Topic, Error));
  }

  std::vector<int64_t> CurrentOffsets;
  for (auto TopicPartition : TopicPartitions) {
    // cppcheck-suppress useStlAlgorithm
    CurrentOffsets.push_back(TopicPartition->offset());
  }
  return CurrentOffsets;
}

std::vector<RdKafka::TopicPartition *>
Consumer::offsetsForTimesForTopic(std::string const &Topic,
                                  std::chrono::milliseconds const Time) {
  auto NumberOfPartitions = getNumberOfPartitionsInTopic(Topic);
  std::vector<RdKafka::TopicPartition *> TopicPartitionsWithTimestamp;
  for (uint64_t i = 0; i < NumberOfPartitions; i++) {
    auto TopicPartition = RdKafka::TopicPartition::create(Topic, i);
    TopicPartition->set_offset(Time.count());
    TopicPartitionsWithTimestamp.push_back(TopicPartition);
  }

  uint32_t LoopCounter = 0;
  uint32_t WarnOnNRetries = 10;
  while (!queryOffsetsForTimes(TopicPartitionsWithTimestamp)) {
    if (LoopCounter == WarnOnNRetries) {
      Logger->warn("Cannot contact broker, retrying until connection is "
                   "established...");
      LoopCounter = 0;
    }
    LoopCounter++;
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
  Logger->debug("Successfully queried offsets for times");

  return TopicPartitionsWithTimestamp;
}

/// Returns the number of partitions in the topic, if the provided Topic name is
/// the current assignment then number of partitions is already known and we can
/// avoid a metadata request.
///
/// \param Topic Name of the topic.
/// \return Number of partitions in the named topic.
size_t Consumer::getNumberOfPartitionsInTopic(const std::string &Topic) {
  size_t NumberOfPartitions;
  if (Topic == CurrentTopic && CurrentNumberOfPartitions != 0) {
    NumberOfPartitions = CurrentNumberOfPartitions;
  } else {
    NumberOfPartitions = queryTopicPartitions(Topic).size();
  }
  return NumberOfPartitions;
}

/// Get offsets for times, returns true if successful and false otherwise
/// Timestamps in provided topic partitions' offset field will be replaced with
/// offset if successful.
///
/// \param TopicPartitionsWithTimestamp topic partitions container timestamp in
/// the offset field.
/// \return true if successful.
bool Consumer::queryOffsetsForTimes(
    std::vector<RdKafka::TopicPartition *> &TopicPartitionsWithTimestamp) {
  auto ErrorCode = KafkaConsumer->offsetsForTimes(
      TopicPartitionsWithTimestamp,
      ConsumerBrokerSettings.OffsetsForTimesTimeoutMS);

  switch (ErrorCode) {
  case RdKafka::ERR_NO_ERROR:
    return true;
  case RdKafka::ERR__TRANSPORT:
    return false;
  case RdKafka::ERR__TIMED_OUT:
    return false;
  default:
    Logger->error("Kafka error while getting offsets for timestamp: {}",
                  ErrorCode);
    throw std::runtime_error(fmt::format(
        "Kafka error while getting offsets for timestamp: {}", ErrorCode));
  }
}

std::vector<int64_t>
Consumer::offsetsForTimesAllPartitions(std::string const &Topic,
                                       std::chrono::milliseconds const Time) {
  auto TopicPartitions = offsetsForTimesForTopic(Topic, Time);
  std::vector<int64_t> Offsets(TopicPartitions.size(), 0);
  for (auto TopicPartition : TopicPartitions) {
    Offsets[TopicPartition->partition()] = TopicPartition->offset();
  }
  return Offsets;
}

void Consumer::addTopicAtTimestamp(std::string const &Topic,
                                   std::chrono::milliseconds const StartTime) {
  Logger->info("Consumer::addTopicAtTimestamp  Topic: {}  StartTime: {}", Topic,
               StartTime.count());

  auto TopicPartitions = offsetsForTimesForTopic(Topic, StartTime);
  assignToPartitions(Topic, TopicPartitions);
}

int64_t Consumer::getHighWatermarkOffset(std::string const &Topic,
                                         int32_t Partition) {
  int64_t LowWatermark, HighWatermark;
  // Note, does not query broker
  KafkaConsumer->get_watermark_offsets(Topic, Partition, &LowWatermark,
                                       &HighWatermark);
  return HighWatermark;
}

std::vector<int32_t> Consumer::queryTopicPartitions(const std::string &Topic) {
  std::shared_ptr<RdKafka::Metadata> KafkaMetadata = getMetadata();
  auto matchedTopic = findTopic(Topic, KafkaMetadata);
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

bool Consumer::topicPresent(const std::string &TopicName) {
  try {
    std::shared_ptr<RdKafka::Metadata> KafkaMetadata = getMetadata();
    findTopic(TopicName, KafkaMetadata);
  } catch (std::runtime_error &e) {
    return false;
  }
  return true;
}

/// Gets metadata from the Kafka broker, if unsuccessful then keeps trying,
/// and logs a warning message every WarnOnNRetries attempts.
std::shared_ptr<RdKafka::Metadata> Consumer::getMetadata() {
  Logger->trace("Querying broker for Metadata");
  uint32_t LoopCounter = 0;
  uint32_t WarnOnNRetries = 10;
  std::shared_ptr<RdKafka::Metadata> KafkaMetadata = metadataCall();
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

std::shared_ptr<RdKafka::Metadata> Consumer::metadataCall() {
  RdKafka::Metadata *MetadataPtr = nullptr;
  auto ErrorCode = KafkaConsumer->metadata(
      true, nullptr, &MetadataPtr, ConsumerBrokerSettings.MetadataTimeoutMS);
  switch (ErrorCode) {
  case RdKafka::ERR_NO_ERROR:
    return std::shared_ptr<RdKafka::Metadata>(MetadataPtr);
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

std::unique_ptr<std::pair<PollStatus, FileWriter::Msg>> Consumer::poll() {
  auto KafkaMsg = std::unique_ptr<RdKafka::Message>(
      KafkaConsumer->consume(ConsumerBrokerSettings.PollTimeoutMS));
  auto DataToReturn =
      std::make_unique<std::pair<PollStatus, FileWriter::Msg>>();

  switch (KafkaMsg->err()) {
  case RdKafka::ERR_NO_ERROR:
    if (KafkaMsg->len() > 0) {
      DataToReturn->first = PollStatus::Message;
      // extract data
      DataToReturn->second = FileWriter::Msg::owned(
          reinterpret_cast<const char *>(KafkaMsg->payload()), KafkaMsg->len());
      DataToReturn->second.MetaData = FileWriter::MessageMetaData{
          std::chrono::milliseconds(KafkaMsg->timestamp().timestamp),
          KafkaMsg->timestamp().type, KafkaMsg->offset(),
          KafkaMsg->partition()};

      return DataToReturn;
    } else {
      DataToReturn->first = PollStatus::Empty;
      return DataToReturn;
    }
  case RdKafka::ERR__TIMED_OUT:
    // No message or event within time out - this is usually normal (see
    // librdkafka docs)
    DataToReturn->first = PollStatus::TimedOut;
    return DataToReturn;
  case RdKafka::ERR__PARTITION_EOF:
    DataToReturn->first = PollStatus::EndOfPartition;
    return DataToReturn;
  default:
    // Everything else is an error
    DataToReturn->first = PollStatus::Error;
    return DataToReturn;
  }
}
} // namespace KafkaW
