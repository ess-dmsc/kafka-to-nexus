#include "Consumer.h"
#include "logger.h"
#include <atomic>

namespace KafkaW {

static std::atomic<int> g_kafka_consumer_instance_count;

#define KERR(rk, err)                                                          \
  if (err != 0) {                                                              \
    LOG(Sev::Error, "Kafka {}  error: {}, {}, {}", rd_kafka_name(rk), err,     \
        rd_kafka_err2name((rd_kafka_resp_err_t)err),                           \
        rd_kafka_err2str((rd_kafka_resp_err_t)err));                           \
  }

Consumer::Consumer(BrokerSettings BrokerSettings, ConsumerSettings CSettings)
    : ConsumerBrokerSettings(BrokerSettings), Settings(CSettings) {
  init();
  id = g_kafka_consumer_instance_count++;
}

Consumer::~Consumer() {
  LOG(Sev::Debug, "~Consumer()");
  if (RdKafka != nullptr) {
    LOG(Sev::Debug, "rd_kafka_consumer_close");
    rd_kafka_consumer_close(RdKafka);
    LOG(Sev::Debug, "rd_kafka_destroy");
    rd_kafka_destroy(RdKafka);
    RdKafka = nullptr;
  }
  if (PartitionList != nullptr) {
    rd_kafka_topic_partition_list_destroy(PartitionList);
    PartitionList = nullptr;
  }
}

void Consumer::cb_log(rd_kafka_t const *rk, int level, char const *fac,
                      char const *buf) {
  auto self = reinterpret_cast<Consumer *>(rd_kafka_opaque(rk));
  LOG(Sev(level), "IID: {}  {}  fac: {}", self->id, buf, fac);
}

void Consumer::cb_error(rd_kafka_t *, int err_i, char const *msg,
                        void *opaque) {
  auto self = reinterpret_cast<Consumer *>(opaque);
  auto err = static_cast<rd_kafka_resp_err_t>(err_i);
  Sev ll = Sev::Debug;
  if (err == RD_KAFKA_RESP_ERR__TRANSPORT) {
    ll = Sev::Warning;
    // rd_kafka_dump(stdout, rk);
  }
  LOG(ll, "Kafka cb_error id: {}  broker: {}  errno: {}  errorname: {}  "
          "errorstring: {}  message: {}",
      self->id, self->ConsumerBrokerSettings.Address, err_i,
      rd_kafka_err2name(err), rd_kafka_err2str(err), msg);
}

int Consumer::cb_stats(rd_kafka_t *, char *json, size_t json_size, void *) {
  LOG(Sev::Debug, "INFO stats_cb {}  {:.{}}", json_size, json, json_size);
  // What does Kafka want us to return from this callback?
  return 0;
}

static void print_partition_list(rd_kafka_topic_partition_list_t *plist) {
  for (int i1 = 0; i1 < plist->cnt; ++i1) {
    auto &x = plist->elems[i1];
    LOG(Sev::Debug, "   {}  {}  {}", x.topic, x.partition, x.offset);
  }
}

void Consumer::cb_rebalance(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                            rd_kafka_topic_partition_list_t *plist,
                            void *opaque) {
  rd_kafka_resp_err_t err2;
  auto self = static_cast<Consumer *>(opaque);
  switch (err) {
  case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
    LOG(Sev::Debug, "cb_rebalance assign {}", rd_kafka_name(rk));
    if (auto &cb = self->on_rebalance_start) {
      cb(plist);
    }
    print_partition_list(plist);
    err2 = rd_kafka_assign(rk, plist);
    if (err2 != RD_KAFKA_RESP_ERR_NO_ERROR) {
      LOG(Sev::Notice, "rebalance error: {}  {}", rd_kafka_err2name(err2),
          rd_kafka_err2str(err2));
    }
    if (auto &cb = self->on_rebalance_assign) {
      cb(plist);
    }
    break;
  case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
    LOG(Sev::Warning, "cb_rebalance revoke:");
    print_partition_list(plist);
    err2 = rd_kafka_assign(rk, nullptr);
    if (err2 != RD_KAFKA_RESP_ERR_NO_ERROR) {
      LOG(Sev::Warning, "rebalance error: {}  {}", rd_kafka_err2name(err2),
          rd_kafka_err2str(err2));
    }
    break;
  default:
    LOG(Sev::Info, "cb_rebalance failure and revoke: {}",
        rd_kafka_err2str(err));
    err2 = rd_kafka_assign(rk, nullptr);
    if (err2 != RD_KAFKA_RESP_ERR_NO_ERROR) {
      LOG(Sev::Warning, "rebalance error: {}  {}", rd_kafka_err2name(err2),
          rd_kafka_err2str(err2));
    }
    break;
  }
}

void Consumer::init() {
  // librdkafka API sometimes wants to write errors into a buffer:
  int const errstr_N = 512;
  char errstr[errstr_N];

  auto conf = rd_kafka_conf_new();
  ConsumerBrokerSettings.apply(conf);

  rd_kafka_conf_set_log_cb(conf, Consumer::cb_log);
  rd_kafka_conf_set_error_cb(conf, Consumer::cb_error);
  rd_kafka_conf_set_stats_cb(conf, Consumer::cb_stats);
  rd_kafka_conf_set_rebalance_cb(conf, Consumer::cb_rebalance);
  rd_kafka_conf_set_consume_cb(conf, nullptr);
  rd_kafka_conf_set_opaque(conf, this);

  RdKafka = rd_kafka_new(RD_KAFKA_CONSUMER, conf, static_cast<char *>(errstr),
                         errstr_N);
  if (RdKafka == nullptr) {
    LOG(Sev::Error, "can not create kafka handle: {}", errstr);
    throw std::runtime_error("can not create Kafka handle");
  }

  rd_kafka_set_log_level(RdKafka, 4);

  LOG(Sev::Info, "New Kafka consumer {} with brokers: {}",
      rd_kafka_name(RdKafka), ConsumerBrokerSettings.Address.c_str());
  if (rd_kafka_brokers_add(RdKafka, ConsumerBrokerSettings.Address.c_str()) ==
      0) {
    LOG(Sev::Error, "could not add brokers");
    throw std::runtime_error("could not add brokers");
  }

  rd_kafka_poll_set_consumer(RdKafka);

  // Allocate some default size.  This is not a limit.
  PartitionList = rd_kafka_topic_partition_list_new(16);
}

void Consumer::addTopic(std::string const &Topic) {
  LOG(Sev::Info, "Consumer::add_topic  {}", Topic);
  rd_kafka_topic_partition_list_add(PartitionList, Topic.c_str(),
                                    RD_KAFKA_PARTITION_UA);
  int Error = rd_kafka_subscribe(RdKafka, PartitionList);
  KERR(RdKafka, Error);
  if (Error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error("can not subscribe");
  }
}

void Consumer::addTopicAtTimestamp(std::string const &Topic,
                                   std::chrono::milliseconds const StartTime) {
  LOG(Sev::Info, "Consumer::addTopicAtTimestamp  Topic: {}  StartTime: {}",
      Topic, StartTime.count());
  auto numberOfPartitions = queryNumberOfPartitions(Topic);
  rd_kafka_topic_partition_list_add_range(PartitionList, Topic.c_str(), 0,
                                          numberOfPartitions - 1);
  // Set all the timestamps before doing a *single* call to
  // rd_kafka_offsets_for_times
  for (int I = 0; I < PartitionList->cnt; ++I) {
    PartitionList->elems[I].offset = StartTime.count();
  }
  auto Error = rd_kafka_offsets_for_times(RdKafka, PartitionList,
                                          Settings.OffsetsForTimesTimeoutMS);
  if (Error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error(
        fmt::format("Error from rd_kafka_offsets_for_times {}", Error));
  }
  for (int I = 0; I < PartitionList->cnt; ++I) {
    LOG(Sev::Debug, "Topic: {}, Partition: {}, Offset: {}", Topic,
        PartitionList->elems[I].partition, PartitionList->elems[I].offset);
  }
  commitOffsets();
  Error = rd_kafka_subscribe(RdKafka, PartitionList);
  KERR(RdKafka, Error);
  if (Error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error(
        fmt::format("Error from rd_kafka_subscribe {}", Error));
  }
}

void Consumer::commitOffsets() const {
  auto CommitErr = rd_kafka_commit(RdKafka, PartitionList, 0); // Blocking call
  KERR(RdKafka, CommitErr);
  if (CommitErr == RD_KAFKA_RESP_ERR__NO_OFFSET) {
    LOG(Sev::Warning, "Could not commit offsets in Consumer, possibly already "
                      "at the correct offset");
    return;
  }
  if (CommitErr != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error("Could not commit offsets in Consumer");
  }
}

int32_t Consumer::queryNumberOfPartitions(const std::string &TopicName) {
  const rd_kafka_metadata_t *Metadata = nullptr;
  int QueryAllTopicsInCluster = 1;
  auto Error = rd_kafka_metadata(RdKafka, QueryAllTopicsInCluster, nullptr,
                                 &Metadata, Settings.MetadataTimeoutMS);
  if (Error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error("Error in queryNumberOfPartitions");
  }
  for (int I = 0; I < Metadata->topic_cnt; ++I) {
    if (TopicName == Metadata->topics[I].topic) {
      auto Count = Metadata->topics[I].partition_cnt;
      rd_kafka_metadata_destroy(Metadata);
      return Count;
    }
  }
  rd_kafka_metadata_destroy(Metadata);
  throw std::runtime_error(
      fmt::format("Topic {} not found by queryNumberOfPartitions", TopicName));
}

bool Consumer::topicPresent(const std::string &Topic) {
  const rd_kafka_metadata_t *Metadata{nullptr};
  rd_kafka_metadata(RdKafka, 1, nullptr, &Metadata, Settings.MetadataTimeoutMS);

  bool IsPresent = false;
  if (Metadata == nullptr) {
    throw std::runtime_error("could not create metadata");
  }

  for (int TopicNr = 0; TopicNr < Metadata->topic_cnt; ++TopicNr) {
    if (Metadata->topics[TopicNr].topic == Topic) {
      IsPresent = true;
      break;
    }
  }
  rd_kafka_metadata_destroy(Metadata);
  return IsPresent;
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
        reinterpret_cast<std::uint8_t *>(msg->payload), msg->len,
        [msg]() { rd_kafka_message_destroy(msg); }, msg->offset);
  }
  if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
    return std::make_unique<ConsumerMessage>(PollStatus::EOP);
  }
  if (msg->err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
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
