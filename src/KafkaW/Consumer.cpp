#include "Consumer.h"
#include "MetadataException.h"
#include "logger.h"
#include <atomic>
#include <iostream>

namespace KafkaW {

static std::atomic<int> g_kafka_consumer_instance_count;

#define KERR(rk, err)                                                          \
  if (err != 0) {                                                              \
    LOG(Sev::Error, "Kafka {}  error: {}, {}, {}", rd_kafka_name(rk), err,     \
        rd_kafka_err2name((rd_kafka_resp_err_t)err),                           \
        rd_kafka_err2str((rd_kafka_resp_err_t)err));                           \
  }

Consumer::Consumer(const BrokerSettings &BrokerSettings)
    : ConsumerBrokerSettings(std::move(BrokerSettings)) {
  ////C++
  std::string ErrorString;
  auto conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  conf->set("rebalance_cb", &RebalanceCallback, ErrorString);
  conf->set("event_cb", &EventCallback, ErrorString);
  conf->set("metadata.broker.list", ConsumerBrokerSettings.Address,
            ErrorString);
  ConsumerBrokerSettings.apply(conf);
  this->KafkaConsumer = std::shared_ptr<RdKafka::KafkaConsumer>(
      RdKafka::KafkaConsumer::create(conf, ErrorString));
  if (!this->KafkaConsumer) {
    LOG(Sev::Error, "can not create kafka consumer: {}", ErrorString);
    throw std::runtime_error("can not create Kafka consumer");
  }
  ////C++__

  //  // librdkafka API sometimes wants to write errors into a buffer:
  //  int const errstr_N = 512;
  //  char errstr[errstr_N];
  //
  //  auto conf = rd_kafka_conf_new();
  //  ConsumerBrokerSettings.apply(conf);
  //
  //  rd_kafka_conf_set_log_cb(conf, Consumer::cb_log);
  //  rd_kafka_conf_set_error_cb(conf, Consumer::cb_error);
  //  rd_kafka_conf_set_stats_cb(conf, Consumer::cb_stats);
  //  rd_kafka_conf_set_rebalance_cb(conf, Consumer::cb_rebalance);
  //  rd_kafka_conf_set_consume_cb(conf, nullptr);
  //  rd_kafka_conf_set_opaque(conf, this);
  //
  //  RdKafka = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, errstr_N);
  //  if (!RdKafka) {
  //    LOG(Sev::Error, "can not create kafka handle: {}", errstr);
  //    throw std::runtime_error("can not create Kafka handle");
  //  }
  //
  //  rd_kafka_set_log_level(RdKafka, 4);
  //
  //  LOG(Sev::Info, "New Kafka consumer {} with brokers: {}",
  //      rd_kafka_name(RdKafka), ConsumerBrokerSettings.Address.c_str());
  //  if (rd_kafka_brokers_add(RdKafka, ConsumerBrokerSettings.Address.c_str())
  //  ==
  //      0) {
  //    LOG(Sev::Error, "could not add brokers");
  //    throw std::runtime_error("could not add brokers");
  //  }
  //
  //  rd_kafka_poll_set_consumer(RdKafka);
  //
  //  // Allocate some default size.  This is not a limit.
  //  PartitionList = rd_kafka_topic_partition_list_new(16);
  id = g_kafka_consumer_instance_count++;
}
////C++ READY
Consumer::~Consumer() {
  LOG(Sev::Debug, "~Consumer()");
  if (KafkaConsumer) {
    LOG(Sev::Debug, "Close the consumer");
    this->KafkaConsumer->close();
    RdKafka::wait_destroyed(5000);
  }
}
////C++ READY
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
    TopicPartition->set_offset(Low);
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

  dumpCurrentSubscription();
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

  auto ErrorCode =
      KafkaConsumer->offsetsForTimes(TopicPartitionsWithTimestamp, 1000);
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

  //  rd_kafka_topic_partition_list_add_range(PartitionList, Topic.c_str(), 0,
  //                                          numberOfPartitions - 1);
  //  // Set all the timestamps before doing a *single* call to
  //  // rd_kafka_offsets_for_times
  //  for (int I = 0; I < PartitionList->cnt; ++I) {
  //    PartitionList->elems[I].offset = StartTime.count();
  //  }
  //  int Timeout = 1000;
  //  auto Error = rd_kafka_offsets_for_times(RdKafka, PartitionList, Timeout);
  //  if (Error != RD_KAFKA_RESP_ERR_NO_ERROR) {
  //    throw std::runtime_error(
  //        fmt::format("Error from rd_kafka_offsets_for_times {}", Error));
  //  }
  //  for (int I = 0; I < PartitionList->cnt; ++I) {
  //    LOG(Sev::Debug, "Topic: {}, Partition: {}, Offset: {}", Topic,
  //        PartitionList->elems[I].partition, PartitionList->elems[I].offset);
  //  }
  //  commitOffsets();
  //  Error = rd_kafka_subscribe(RdKafka, PartitionList);
  //  KERR(RdKafka, Error);
  //  if (Error != RD_KAFKA_RESP_ERR_NO_ERROR) {
  //    throw std::runtime_error(
  //        fmt::format("Error from rd_kafka_subscribe {}", Error));
  //  }
}

// void Consumer::commitOffsets() const {
//  auto CommitErr = rd_kafka_commit(RdKafka, PartitionList, false);
//  KERR(RdKafka, CommitErr);
//  if (CommitErr == RD_KAFKA_RESP_ERR__NO_OFFSET) {
//    LOG(Sev::Warning, "Could not commit offsets in Consumer, possibly already
//    "
//                      "at the correct offset");
//    return;
//  }
//  if (CommitErr != RD_KAFKA_RESP_ERR_NO_ERROR) {
//    throw std::runtime_error("Could not commit offsets in Consumer");
//  }
//}

////C++ READY
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
  // save needed partition metadata here
  std::vector<int32_t> TopicPartitionNumbers;

  for (auto &Partition : *MatchedTopic->partitions()) {
    TopicPartitionNumbers.push_back(Partition->id());
  }
  sort(TopicPartitionNumbers.begin(), TopicPartitionNumbers.end());
  return TopicPartitionNumbers;
}

////C++ READY
bool Consumer::topicPresent(const std::string &TopicName) {

  auto Metadata = queryMetadata();
  auto Topics = Metadata->topics();
  for (auto Topic : *Topics)
    if (Topic->topic() == TopicName)
      return true;
  return false;
}

////C++ READY
void Consumer::dumpCurrentSubscription() {
  std::vector<std::string> TopicList;
  auto ErrorString = KafkaConsumer->subscription(TopicList);
  if (ErrorString == 0) {
    LOG(Sev::Info, "Subscribed topics: ")
    for (auto Topic : TopicList) {
      LOG(Sev::Info, "{}", Topic);
      std::cout << Topic << std::endl;
    }
  } else
    std::cout << ErrorString << std::endl;
}

////C++ READY
std::unique_ptr<RdKafka::Metadata> Consumer::queryMetadata() {
  RdKafka::Metadata *metadataRawPtr(nullptr);
  KafkaConsumer->metadata(true, nullptr, &metadataRawPtr, 1000);
  std::unique_ptr<RdKafka::Metadata> metadata(metadataRawPtr);
  if (metadata == nullptr) {
    throw MetadataException("Failed to query metadata from broker!");
  }
  return metadata;
}

////C++ READY
std::unique_ptr<ConsumerMessage> Consumer::poll() {
  using std::make_unique;
  auto KafkaMsg = std::unique_ptr<RdKafka::Message>(
      KafkaConsumer->consume(ConsumerBrokerSettings.PollTimeoutMS));
  switch (KafkaMsg->err()) {
  case RdKafka::ERR_NO_ERROR:
    if (KafkaMsg->len() > 0) {
      return make_unique<ConsumerMessage>((std::uint8_t *)KafkaMsg->payload(),
                                          KafkaMsg->len(), PollStatus::Msg);
    } else {
      return make_unique<ConsumerMessage>(PollStatus::Empty);
    }
  case RdKafka::ERR__PARTITION_EOF:
    return make_unique<ConsumerMessage>(PollStatus::EOP);
  default:
    return make_unique<ConsumerMessage>(PollStatus::Err);
  }
}

} // namespace KafkaW
