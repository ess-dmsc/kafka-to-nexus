#include "Consumer.h"
#include "MetadataException.h"
#include "Msg.h"
#include "logger.h"
#include <atomic>
#include <chrono>

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
  LOG(Sev::Debug, "~Consumer()");
  if (KafkaConsumer != nullptr) {
    LOG(Sev::Debug, "Close the consumer");
    KafkaConsumer->close();
    RdKafka::wait_destroyed(5000);
  }
}

void Consumer::addTopic(const std::string &Topic) {
  LOG(Sev::Info, "Consumer::add_topic  {}", Topic);
  std::vector<RdKafka::TopicPartition *> TopicPartitionsWithOffsets;
  auto PartitionIDs = queryTopicPartitions(Topic);
  for (int PartitionID : PartitionIDs) {
    auto TopicPartition = RdKafka::TopicPartition::create(Topic, PartitionID);
    int64_t Low, High;
    KafkaConsumer->query_watermark_offsets(Topic, PartitionID, &Low, &High,
                                           100);
    TopicPartition->set_offset(High);
    TopicPartitionsWithOffsets.push_back(TopicPartition);
  }
  RdKafka::ErrorCode ERR = KafkaConsumer->assign(TopicPartitionsWithOffsets);
  if (ERR != 0) {
    LOG(Sev::Error, "Could not assign to {}", Topic);
    throw std::runtime_error(fmt::format("Could not assign to {}", Topic));
  }
  std::for_each(TopicPartitionsWithOffsets.cbegin(),
                TopicPartitionsWithOffsets.cend(),
                [](RdKafka::TopicPartition *Partition) { delete Partition; });
}

void Consumer::addTopicAtTimestamp(std::string const &Topic,
                                   std::chrono::milliseconds const StartTime) {
  LOG(Sev::Info, "Consumer::addTopicAtTimestamp  Topic: {}  StartTime: {}",
      Topic, StartTime.count());
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
    LOG(Sev::Error, "Kafka error while getting offsets for timestamp: {}",
        ErrorCode);
    throw std::runtime_error(fmt::format(
        "Kafka error while getting offsets for timestamp: {}", ErrorCode));
  }

  ErrorCode = KafkaConsumer->assign(TopicPartitionsWithTimestamp);
  std::for_each(TopicPartitionsWithTimestamp.cbegin(),
                TopicPartitionsWithTimestamp.cend(),
                [](RdKafka::TopicPartition *Partition) { delete Partition; });
  if (ErrorCode != RdKafka::ErrorCode::ERR_NO_ERROR) {
    LOG(Sev::Error,
        "Kafka error while subscribing to offsets from timestamp: {}",
        ErrorCode);
    throw std::runtime_error(fmt::format(
        "Kafka error while subscribing to offsets from timestamp: {}",
        ErrorCode));
  }
}

const RdKafka::TopicMetadata *Consumer::findTopic(const std::string &Topic) {
  updateMetadata();
  auto Topics = KafkaMetadata->topics();
  auto Iterator =
      std::find_if(Topics->cbegin(), Topics->cend(),
                   [Topic](const RdKafka::TopicMetadata *TopicMetadata) {
                     return TopicMetadata->topic() == Topic;
                   });
  if (Iterator == Topics->end()) {
    throw std::runtime_error("Config topic does not exist");
  }
  return *Iterator;
}

std::vector<int32_t>
Consumer::queryTopicPartitions(const std::string &TopicName) {
  auto matchedTopic = findTopic(TopicName);
  std::vector<int32_t> TopicPartitionNumbers;
  const RdKafka::TopicMetadata::PartitionMetadataVector *PartitionMetadata =
      matchedTopic->partitions();
  for (const auto &Partition : *PartitionMetadata) {
    TopicPartitionNumbers.push_back(Partition->id());
  }
  sort(TopicPartitionNumbers.begin(), TopicPartitionNumbers.end());
  return TopicPartitionNumbers;
}

bool Consumer::topicPresent(const std::string &TopicName) {
  updateMetadata();
  for (auto Topic : *KafkaMetadata->topics())
    if (Topic->topic() == TopicName)
      return true;
  return false;
}

void Consumer::dumpCurrentSubscription() {
  std::vector<RdKafka::TopicPartition *> Partitions;
  auto ErrorString = KafkaConsumer->assignment(Partitions);
  if (ErrorString == 0) {
    LOG(Sev::Info, "Assigned partitions and offsets:")
    for (auto TopicPartition : Partitions) {
      LOG(Sev::Info, "{}: {}, {}", TopicPartition->topic(),
          TopicPartition->partition(), TopicPartition->offset());
    }
  } else
    LOG(Sev::Error, "Cannot display assigned partitions: {}", ErrorString);
}

void Consumer::updateMetadata() {
  RdKafka::Metadata *MetadataPtr = nullptr;
  auto RetCode = KafkaConsumer->metadata(
      true, nullptr, &MetadataPtr, ConsumerBrokerSettings.MetadataTimeoutMS);
  if (RetCode != RdKafka::ERR_NO_ERROR) {
    throw MetadataException(
        "Consumer::updateMetadata() - error while retrieving metadata.");
  }
  KafkaMetadata = std::shared_ptr<RdKafka::Metadata>(MetadataPtr);
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
