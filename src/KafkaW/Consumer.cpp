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
#include "logger.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>

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
    Logger->debug("Close the consumer");
    KafkaConsumer->close();
    RdKafka::wait_destroyed(ConsumerBrokerSettings.ConsumerCloseTimeoutMS);
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
void Consumer::assignToPartitions(const std::string &Topic,
                                  const std::vector<RdKafka::TopicPartition *>
                                      &TopicPartitionsWithOffsets) const {
  RdKafka::ErrorCode ErrorCode =
      KafkaConsumer->assign(TopicPartitionsWithOffsets);
  for_each(TopicPartitionsWithOffsets.cbegin(),
           TopicPartitionsWithOffsets.cend(),
           [](RdKafka::TopicPartition *Partition) { delete Partition; });
  if (ErrorCode != RdKafka::ERR_NO_ERROR) {
    Logger->error("Could not assign to {}", Topic);
    throw std::runtime_error(fmt::v5::format("Could not assign to {}", Topic));
  }
}

void Consumer::addTopicAtTimestamp(std::string const &Topic,
                                   std::chrono::milliseconds const StartTime) {
  Logger->info("Consumer::addTopicAtTimestamp  Topic: {}  StartTime: {}", Topic,
               StartTime.count());
  auto NumberOfPartitions = queryTopicPartitions(Topic).size();
  std::vector<RdKafka::TopicPartition *> TopicPartitionsWithTimestamp;
  for (unsigned int i = 0; i < NumberOfPartitions; i++) {
    auto TopicPartition = RdKafka::TopicPartition::create(Topic, i);

    TopicPartition->set_offset(StartTime.count());
    TopicPartitionsWithTimestamp.push_back(TopicPartition);
  }

  auto ErrorCode = KafkaConsumer->offsetsForTimes(
      TopicPartitionsWithTimestamp,
      ConsumerBrokerSettings.OffsetsForTimesTimeoutMS);
  if (ErrorCode != RdKafka::ErrorCode::ERR_NO_ERROR) {
    Logger->error("Kafka error while getting offsets for timestamp: {}",
                  ErrorCode);
    throw std::runtime_error(fmt::format(
        "Kafka error while getting offsets for timestamp: {}", ErrorCode));
  }
  assignToPartitions(Topic, TopicPartitionsWithTimestamp);
}

/// Checks if a topic is present on the broker. Throws and logs if topic is
/// not found.
/// NB this is not put in a smart pointer as we don't want to take ownership.
/// \param Topic Name of the topic to check.
/// \return The topic metadata object.
const RdKafka::TopicMetadata *Consumer::findTopic(const std::string &Topic) {
  updateMetadata();
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

std::vector<int32_t> Consumer::queryTopicPartitions(const std::string &Topic) {
  auto matchedTopic = findTopic(Topic);
  std::vector<int32_t> TopicPartitionNumbers;
  const RdKafka::TopicMetadata::PartitionMetadataVector *PartitionMetadata =
      matchedTopic->partitions();
  std::transform(PartitionMetadata->begin(), PartitionMetadata->end(),
                 std::back_inserter(TopicPartitionNumbers),
                 [](auto Partition) { return Partition->id(); });
  sort(TopicPartitionNumbers.begin(), TopicPartitionNumbers.end());
  return TopicPartitionNumbers;
}

bool Consumer::topicPresent(const std::string &TopicName) {
  try {
    findTopic(TopicName);
  } catch (std::runtime_error &e) {
    return false;
  };
  return true;
}

/// Updates the RdKafka Metadata pointer. If broker is unavailable, keeps trying
/// to connect every 500ms, logging messages every few seconds.
void Consumer::updateMetadata() {
  short LoopCounter = 0;
  while (!metadataCall()) {
    if (LoopCounter == 7) {
      Logger->error("Cannot contact broker, retrying until connection is "
                    "established...");
      LoopCounter = 0;
    }
    LoopCounter++;
  }
  Logger->info("Connection with broker established.");
}

bool Consumer::metadataCall() {
  RdKafka::Metadata *MetadataPtr = nullptr;
  auto ErrorCode = KafkaConsumer->metadata(
      true, nullptr, &MetadataPtr, ConsumerBrokerSettings.MetadataTimeoutMS);
  switch (ErrorCode) {
  case RdKafka::ERR_NO_ERROR:
    KafkaMetadata = std::shared_ptr<RdKafka::Metadata>(MetadataPtr);
    return true;
  case RdKafka::ERR__TRANSPORT:
    return false;
  default:
    throw MetadataException(
        "Consumer::metadataCall() - error while retrieving metadata.");
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
          KafkaMsg->timestamp().type, KafkaMsg->offset()};

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
