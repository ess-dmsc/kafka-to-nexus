#include "Consumer.h"
#include "MetadataException.h"
#include "Msg.h"
#include "logger.h"
#include <atomic>
#include <chrono>

namespace KafkaW {

static std::atomic<int> ConsumerInstanceCount;

Consumer::Consumer(const BrokerSettings &BrokerSettings)
    : ConsumerBrokerSettings(std::move(BrokerSettings)) {
  std::string ErrorString;
  auto Configuration = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  Configuration->set("rebalance_cb", &RebalanceCallback, ErrorString);
  Configuration->set("event_cb", &EventCallback, ErrorString);
  Configuration->set("metadata.broker.list", ConsumerBrokerSettings.Address,
                     ErrorString);
  ConsumerBrokerSettings.apply(Configuration);
  this->KafkaConsumer = std::shared_ptr<RdKafka::KafkaConsumer>(
      RdKafka::KafkaConsumer::create(Configuration, ErrorString));
  if (!this->KafkaConsumer) {
    LOG(Sev::Error, "can not create kafka consumer: {}", ErrorString);
    throw std::runtime_error("can not create Kafka consumer");
  }
  id = ConsumerInstanceCount++;
}

Consumer::~Consumer() {
  LOG(Sev::Debug, "~Consumer()");
  if (KafkaConsumer) {
    LOG(Sev::Debug, "Close the consumer");
    this->KafkaConsumer->close();
    RdKafka::wait_destroyed(5000);
  }
}

void Consumer::addTopic(const std::string Topic) {
  LOG(Sev::Info, "Consumer::add_topic  {}", Topic);
  std::vector<RdKafka::TopicPartition *> TopicPartitionsWithOffsets;
  auto PartitionIDs = queryTopicPartitions(Topic);
  for (unsigned long i = 0; i < PartitionIDs.size(); i++) {
    auto TopicPartition =
        RdKafka::TopicPartition::create(Topic, PartitionIDs[i]);
    int64_t Low, High;
    KafkaConsumer->query_watermark_offsets(Topic, PartitionIDs[i], &Low, &High,
                                           100);
    TopicPartition->set_offset(High);
    TopicPartitionsWithOffsets.push_back(TopicPartition);
  }
  RdKafka::ErrorCode ERR = KafkaConsumer->assign(TopicPartitionsWithOffsets);
  if (ERR != 0) {
    LOG(Sev::Error, "Could not subscribe to {}", Topic);
    throw std::runtime_error(fmt::format("Could not subscribe to {}", Topic));
  }
  std::for_each(TopicPartitionsWithOffsets.cbegin(),
                TopicPartitionsWithOffsets.cend(),
                [](RdKafka::TopicPartition *Partition) { delete Partition; });
}

void Consumer::addTopicAtTimestamp(std::string const Topic,
                                   std::chrono::milliseconds const StartTime) {
  LOG(Sev::Info, "Consumer::addTopicAtTimestamp  Topic: {}  StartTime: {}",
      Topic, StartTime.count());
  auto numberOfPartitions = queryTopicPartitions(Topic).size();
  std::vector<RdKafka::TopicPartition *> TopicPartitionsWithTimestamp;
  for (unsigned int i = 0; i < numberOfPartitions; i++) {
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

std::vector<int32_t>
Consumer::queryTopicPartitions(const std::string &TopicName) {
  auto Metadata = queryMetadata();
  auto Topics = Metadata->topics();
  auto Iterator = std::find_if(Topics->cbegin(), Topics->cend(),
                               [TopicName](const RdKafka::TopicMetadata *tpc) {
                                 return tpc->topic() == TopicName;
                               });
  auto MatchedTopic = *Iterator;
  if (MatchedTopic == nullptr)
    throw MetadataException(fmt::format("No such topic: {}", TopicName));
  std::vector<int32_t> TopicPartitionNumbers;

  for (auto &Partition : *MatchedTopic->partitions()) {
    TopicPartitionNumbers.push_back(Partition->id());
  }
  sort(TopicPartitionNumbers.begin(), TopicPartitionNumbers.end());
  return TopicPartitionNumbers;
}

bool Consumer::topicPresent(const std::string &TopicName) {

  auto Metadata = queryMetadata();
  auto Topics = Metadata->topics();
  for (auto Topic : *Topics)
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

std::unique_ptr<RdKafka::Metadata> Consumer::queryMetadata() {
  RdKafka::Metadata *metadataRawPtr(nullptr);
  KafkaConsumer->metadata(true, nullptr, &metadataRawPtr,
                          ConsumerBrokerSettings.MetadataTimeoutMS);
  std::unique_ptr<RdKafka::Metadata> metadata(metadataRawPtr);
  if (metadata == nullptr) {
    throw MetadataException("Failed to query metadata from broker!");
  }
  return metadata;
}

std::unique_ptr<std::pair<PollStatus, FileWriter::Msg>> Consumer::poll() {
  using std::make_unique;

  auto KafkaMsg = std::unique_ptr<RdKafka::Message>(
      KafkaConsumer->consume(ConsumerBrokerSettings.PollTimeoutMS));

  PollStatus Status;
  FileWriter::Msg KafkaMessage;

  // construct unique ptr to return
  std::pair<PollStatus, FileWriter::Msg> NewPair(Status,
                                                 std::move(KafkaMessage));
  std::unique_ptr<std::pair<PollStatus, FileWriter::Msg>> DataToReturn;
  DataToReturn = std::make_unique<std::pair<PollStatus, FileWriter::Msg>>(
      std::move(NewPair));

  switch (KafkaMsg->err()) {
  case RdKafka::ERR_NO_ERROR:
    if (KafkaMsg->len() > 0) {
      DataToReturn.get()->first = PollStatus::Msg;
      // extract data
      DataToReturn.get()->second = FileWriter::Msg::owned(
          reinterpret_cast<const char *>(KafkaMsg->payload()), KafkaMsg->len());
      DataToReturn.get()->second.MetaData = FileWriter::MessageMetaData{
          KafkaMsg->timestamp().type,
          std::chrono::milliseconds(KafkaMsg->timestamp().timestamp),
          KafkaMsg->offset()};

      return DataToReturn;
    } else {
      DataToReturn.get()->first = PollStatus::Empty;
      return DataToReturn;
    }
  case RdKafka::ERR__PARTITION_EOF:
    DataToReturn.get()->first = PollStatus::EOP;
    return DataToReturn;
  default:
    DataToReturn.get()->first = PollStatus::Err;
    return DataToReturn;
  }
}
} // namespace KafkaW
