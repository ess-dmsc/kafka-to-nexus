#include "ProducerTopic.h"
#include <vector>

namespace KafkaW {

using std::unique_ptr;
using std::shared_ptr;
using std::array;
using std::vector;
using std::string;
using std::atomic;
using std::move;

ProducerTopic::~ProducerTopic() {
  LOG(spdlog::level::trace, "~ProducerTopic {}", TopicName);
  if (RdKafkaTopic) {
    LOG(spdlog::level::trace, "rd_kafka_topic_destroy");
    rd_kafka_topic_destroy(RdKafkaTopic);
    RdKafkaTopic = nullptr;
  }
}

ProducerTopic::ProducerTopic(std::shared_ptr<Producer> Pointer,
                             std::string Topic)
    : ProducerPtr(Pointer), TopicName(std::move(Topic)) {
  TopicSettings TopicSettings;
  rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
  TopicSettings.applySettingsToRdKafkaConf(topic_conf);

  RdKafkaTopic = rd_kafka_topic_new(ProducerPtr->getRdKafkaPtr(),
                                    TopicName.c_str(), topic_conf);
  if (RdKafkaTopic == nullptr) {
    // Seems like Kafka uses the system error code?
    auto errstr = rd_kafka_err2str(rd_kafka_last_error());
    LOG(spdlog::level::err, "could not create Kafka topic: {}", errstr);
    throw TopicCreationError();
  }
  LOG(spdlog::level::trace, "ctor topic: {}  producer: {}",
      rd_kafka_topic_name(RdKafkaTopic),
      rd_kafka_name(ProducerPtr->getRdKafkaPtr()));
}

ProducerTopic::ProducerTopic(ProducerTopic &&x) {
  std::swap(ProducerPtr, x.ProducerPtr);
  std::swap(RdKafkaTopic, x.RdKafkaTopic);
  std::swap(TopicName, x.TopicName);
  std::swap(DoCopyMsg, x.DoCopyMsg);
}

struct Msg_ : public Producer::Msg {
  vector<uchar> v;
  void finalize() {
    data = v.data();
    size = v.size();
  }
};

int ProducerTopic::produce(uchar *MsgData, size_t MsgSize, bool PrintError) {
  auto MsgPtr = new Msg_;
  std::copy(MsgData, MsgData + MsgSize, std::back_inserter(MsgPtr->v));
  MsgPtr->finalize();
  unique_ptr<Producer::Msg> Msg(MsgPtr);
  return produce(Msg);
}

int ProducerTopic::produce(unique_ptr<Producer::Msg> &Msg) {
  if (not RdKafkaTopic) {
    // Should never happen
    return RDKAFKATOPIC_NOT_INITIALIZED;
  }
  int x;
  int32_t partition = RD_KAFKA_PARTITION_UA;
  void const *key = nullptr;
  size_t key_len = 0;
  int msgflags = 0; // 0, RD_KAFKA_MSG_F_COPY, RD_KAFKA_MSG_F_FREE
  x = rd_kafka_produce(RdKafkaTopic, partition, msgflags, Msg->data, Msg->size,
                       key, key_len, Msg.get());

  auto &s = ProducerPtr->Stats;
  if (x != 0) {
    auto err = rd_kafka_last_error();
    if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
      ++s.local_queue_full;
      LOG(spdlog::level::warn, "QUEUE_FULL  outq: {}",
          rd_kafka_outq_len(ProducerPtr->getRdKafkaPtr()));
    } else if (err == RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE) {
      ++s.msg_too_large;
      LOG(spdlog::level::err, "TOO_LARGE  size: {}", Msg->size);
    } else {
      ++s.produce_fail;
      LOG(spdlog::level::trace, "produce topic {}  partition {}   error: {}  {}",
          rd_kafka_topic_name(RdKafkaTopic), partition, x,
          rd_kafka_err2str(err));
    }
  } else {
    ++s.produced;
    s.produced_bytes += (uint64_t)Msg->size;
    ++ProducerPtr->TotalMessagesProduced;
      LOG(spdlog::level::trace, "sent to topic {} partition {}",
          rd_kafka_topic_name(RdKafkaTopic), partition);
    Msg.release();
  }

  return x;
}

void ProducerTopic::enableCopy() { DoCopyMsg = true; }
}
