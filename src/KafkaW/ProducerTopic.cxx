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
  if (rkt) {
    LOG(Sev::Debug, "rd_kafka_topic_destroy");
    rd_kafka_topic_destroy(rkt);
    rkt = nullptr;
  }
}

ProducerTopic::ProducerTopic(std::shared_ptr<Producer> Producer,
                             std::string Name_)
    : Producer_(Producer), Name(Name_) {
  TopicSettings TopicSettings;
  rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
  TopicSettings.applySettingsToRdKafkaConf(topic_conf);

  // rd_kafka_msg_partitioner_random, rd_kafka_msg_partitioner_consistent,
  // rd_kafka_msg_partitioner_consistent_random
  // rd_kafka_topic_conf_set_partitioner_cb(topic_conf,
  // rd_kafka_msg_partitioner_random);

  rkt = rd_kafka_topic_new(Producer_->rd_kafka_ptr(), Name.c_str(), topic_conf);
  if (rkt == nullptr) {
    // Seems like Kafka uses the system error code?
    auto errstr = rd_kafka_err2str(rd_kafka_errno2err(errno));
    LOG(Sev::Error, "could not create Kafka topic: {}", errstr);
    throw std::exception();
  }
  LOG(Sev::Debug, "ctor topic: {}  producer: {}", rd_kafka_topic_name(rkt),
      rd_kafka_name(Producer_->rd_kafka_ptr()));
}

ProducerTopic::ProducerTopic(ProducerTopic &&x) {
  std::swap(Producer_, x.Producer_);
  std::swap(rkt, x.rkt);
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

int ProducerTopic::produce(uchar *msg_data, size_t msg_size, bool print_err) {
  auto p = new Msg_;
  std::copy(msg_data, msg_data + msg_size, std::back_inserter(p->v));
  p->finalize();
  unique_ptr<Producer::Msg> m(p);
  return produce(m);
}

int ProducerTopic::produce(unique_ptr<Producer::Msg> &msg) {
  if (not rkt) {
    throw std::runtime_error("ERROR tried to produce on uninitialized rkt");
  }
  int x;
  int32_t partition = RD_KAFKA_PARTITION_UA;
  void const *key = NULL;
  size_t key_len = 0;
  int msgflags = 0; // 0, RD_KAFKA_MSG_F_COPY, RD_KAFKA_MSG_F_FREE
  x = rd_kafka_produce(rkt, partition, msgflags, msg->data, msg->size, key,
                       key_len, msg.get());

  auto &s = Producer_->stats;
  if (x != 0) {
    auto err = rd_kafka_errno2err(rd_kafka_errno());
    bool print_err = true;
    if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
      ++s.local_queue_full;
      if (print_err) {
        LOG(Sev::Warning, "QUEUE_FULL  outq: {}",
            rd_kafka_outq_len(Producer_->rd_kafka_ptr()));
      }
    } else if (err == RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE) {
      ++s.msg_too_large;
      if (print_err) {
        LOG(Sev::Error, "TOO_LARGE  size: {}", msg->size);
      }
    } else {
      ++s.produce_fail;
      if (print_err) {
        LOG(Sev::Debug, "produce topic {}  partition {}   error: {}  {}",
            rd_kafka_topic_name(rkt), partition, x, rd_kafka_err2str(err));
      }
    }
  } else {
    ++s.produced;
    s.produced_bytes += (uint64_t)msg->size;
    ++Producer_->total_produced_;
    if (log_level >= 8) {
      LOG(Sev::Debug, "sent to topic {} partition {}", rd_kafka_topic_name(rkt),
          partition);
    }
    msg.release();
  }

  return x;
}

void ProducerTopic::enableCopy() { DoCopyMsg = true; }

ProducerMsg::~ProducerMsg() {}

void ProducerMsg::delivery_ok() {}

void ProducerMsg::delivery_fail() {}
}
