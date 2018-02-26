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
  LOG(Sev::Debug, "~ProducerTopic {}", Name);
  if (RdKafkaTopic) {
    LOG(Sev::Debug, "rd_kafka_topic_destroy");
    rd_kafka_topic_destroy(RdKafkaTopic);
    RdKafkaTopic = nullptr;
  }
}

ProducerTopic::ProducerTopic(std::shared_ptr<Producer> Producer,
                             std::string Name_)
    : Producer_(Producer), Name(Name_) {
  TopicSettings TopicSettings;
  rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
  TopicSettings.applySettingsToRdKafkaConf(topic_conf);

  RdKafkaTopic =
      rd_kafka_topic_new(Producer_->getRdKafkaPtr(), Name.c_str(), topic_conf);
  if (RdKafkaTopic == nullptr) {
    // Seems like Kafka uses the system error code?
    auto errstr = rd_kafka_err2str(rd_kafka_last_error());
    LOG(Sev::Error, "could not create Kafka topic: {}", errstr);
    throw TopicCreationError();
  }
  LOG(Sev::Debug, "ctor topic: {}  producer: {}",
      rd_kafka_topic_name(RdKafkaTopic),
      rd_kafka_name(Producer_->getRdKafkaPtr()));
}

ProducerTopic::ProducerTopic(ProducerTopic &&x) {
  std::swap(Producer_, x.Producer_);
  std::swap(RdKafkaTopic, x.RdKafkaTopic);
  std::swap(Name, x.Name);
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
  void const *key = NULL;
  size_t key_len = 0;
  int msgflags = 0; // 0, RD_KAFKA_MSG_F_COPY, RD_KAFKA_MSG_F_FREE
  x = rd_kafka_produce(RdKafkaTopic, partition, msgflags, Msg->data, Msg->size,
                       key, key_len, Msg.get());

  auto &s = Producer_->Stats;
  if (x != 0) {
    auto err = rd_kafka_last_error();
    bool print_err = true;
    if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
      ++s.local_queue_full;
      if (print_err) {
        LOG(Sev::Warning, "QUEUE_FULL  outq: {}",
            rd_kafka_outq_len(Producer_->getRdKafkaPtr()));
      }
    } else if (err == RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE) {
      ++s.msg_too_large;
      if (print_err) {
        LOG(Sev::Error, "TOO_LARGE  size: {}", Msg->size);
      }
    } else {
      ++s.produce_fail;
      if (print_err) {
        LOG(Sev::Debug, "produce topic {}  partition {}   error: {}  {}",
            rd_kafka_topic_name(RdKafkaTopic), partition, x,
            rd_kafka_err2str(err));
      }
    }
  } else {
    ++s.produced;
    s.produced_bytes += (uint64_t)Msg->size;
    ++Producer_->TotalMessagesProduced;
    if (log_level >= 8) {
      LOG(Sev::Debug, "sent to topic {} partition {}",
          rd_kafka_topic_name(RdKafkaTopic), partition);
    }
    Msg.release();
  }

  return x;
}

void ProducerTopic::enableCopy() { DoCopyMsg = true; }
}
