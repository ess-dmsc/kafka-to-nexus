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
  auto ErrCode = KafkaConsumer->subscribe({Topic});
  if (ErrCode != RdKafka::ErrorCode::ERR_NO_ERROR) {
    LOG(Sev::Warning, "Unable to subscribe to topic {} - {}", Topic,
        RdKafka::err2str(ErrCode));
    return;
  };
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
  auto TopicMetadata = getTopicMetadata(TopicName);
  std::vector<int32_t> TopicPartitionNumbers;
  auto PartitionMetadata = TopicMetadata->partitions();
  // save needed partition metadata here
  for (auto &Partition : *PartitionMetadata) {
    TopicPartitionNumbers.push_back(Partition->id());
  }
  sort(TopicPartitionNumbers.begin(), TopicPartitionNumbers.end());
  return TopicPartitionNumbers;
}

////C++ READY
const RdKafka::TopicMetadata *
Consumer::getTopicMetadata(const std::string &Topic) {
  auto Metadata = queryMetadata();
  auto Topics = Metadata->topics();
  auto Iterator = std::find_if(Topics->cbegin(), Topics->cend(),
                               [Topic](const RdKafka::TopicMetadata *tpc) {
                                 return tpc->topic() == Topic;
                               });
  auto MatchedTopic = *Iterator;
  if (MatchedTopic == nullptr)
    throw MetadataException(fmt::format("No such topic: {}", Topic));
  return MatchedTopic;
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
  KafkaConsumer->subscription(TopicList);
  if (!TopicList.empty()) {
    LOG(Sev::Info, "Subscribed topics: ")
    for (auto Topic : TopicList) {
      LOG(Sev::Info, "{}", Topic);
    }
  }
}

////C++ READY
std::unique_ptr<RdKafka::Metadata> Consumer::queryMetadata() {
  RdKafka::Metadata *metadataRawPtr(nullptr);
  KafkaConsumer->metadata(true, nullptr, &metadataRawPtr, 1000);
  std::unique_ptr<RdKafka::Metadata> metadata(metadataRawPtr);
  try {
    if (!metadata) {
      throw MetadataException("Failed to query metadata from broker");
    }
  } catch (std::exception &E) {
    LOG(4, "{}", E.what());
  }
  return metadata;
}

std::unique_ptr<ConsumerMessage> Consumer::poll() {

  auto msg =
      rd_kafka_consumer_poll(RdKafka, ConsumerBrokerSettings.PollTimeoutMS);

  if (msg == nullptr) {
    return std::make_unique<ConsumerMessage>(PollStatus::Empty);
  }

  static_assert(sizeof(char) == 1, "Failed: sizeof(char) == 1");
  if (msg->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    return std::make_unique<ConsumerMessage>(
        (std::uint8_t *)msg->payload, msg->len,
        [msg]() { rd_kafka_message_destroy(msg); }, msg->offset);
  } else if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
    return std::make_unique<ConsumerMessage>(PollStatus::EOP);
  } else if (msg->err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
    LOG(Sev::Error, "RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN");
  } else if (msg->err == RD_KAFKA_RESP_ERR__BAD_MSG) {
    LOG(Sev::Error, "RD_KAFKA_RESP_ERR__BAD_MSG");
  } else if (msg->err == RD_KAFKA_RESP_ERR__DESTROY) {
    LOG(Sev::Error, "RD_KAFKA_RESP_ERR__DESTROY");
    // Broker will go away soon
  } else {
    LOG(Sev::Error, "unhandled msg error: {} {}", rd_kafka_err2name(msg->err),
        rd_kafka_err2str(msg->err));
  }
  return std::make_unique<ConsumerMessage>(PollStatus::Err);
}

} // namespace KafkaW
