#include "ProducerTopic.h"
#include <helper.h>
#include <vector>

namespace KafkaW {

ProducerTopic::ProducerTopic(std::shared_ptr<Producer> ProducerPtr,
                             std::string TopicName)
    : KafkaProducer(ProducerPtr), Name(std::move(TopicName)) {

  std::string ErrStr;
  Config = std::unique_ptr<RdKafka::Conf>(
      RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
  RdKafkaTopic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(
      KafkaProducer->getRdKafkaPtr(), Name, Config.get(), ErrStr));
  if (RdKafkaTopic == nullptr) {
    LOG(Sev::Error, "could not create Kafka topic: {}", ErrStr);
    throw TopicCreationError();
  }
  LOG(Sev::Debug, "ctor topic: {}", RdKafkaTopic->name());
}

struct Msg_ : public ProducerMessage {
  std::vector<unsigned char> v;
  void finalize() {
    data = v.data();
    size = v.size();
  }
};

int ProducerTopic::produce(unsigned char *MsgData, size_t MsgSize) {
  auto MsgPtr = new Msg_;
  std::copy(MsgData, MsgData + MsgSize, std::back_inserter(MsgPtr->v));
  MsgPtr->finalize();
  std::unique_ptr<ProducerMessage> Msg(MsgPtr);
  return produce(Msg);
}

int ProducerTopic::produce(std::unique_ptr<ProducerMessage> &Msg) {
  void const *key = nullptr;
  size_t key_len = 0;
  // MsgFlags = 0 means that we are responsible for cleaning up the message
  // after it has been sent
  // We do this by providing a pointer to our message object in the produce
  // call, this pointer is returned to us in the delivery callback, at which
  // point we can free the memory
  int MsgFlags = 0;
  auto &ProducerStats = KafkaProducer->Stats;

  switch (KafkaProducer->produce(
      RdKafkaTopic.get(), RdKafka::Topic::PARTITION_UA, MsgFlags, Msg->data,
      Msg->size, key, key_len, Msg.get())) {
  case RdKafka::ERR_NO_ERROR:
    ++ProducerStats.produced;
    ProducerStats.produced_bytes += static_cast<uint64_t>(Msg->size);
    ++KafkaProducer->TotalMessagesProduced;
    Msg.release(); // we clean up the message after it has been sent, see
                   // comment by MsgFlags declaration
    return 0;

  case RdKafka::ERR__QUEUE_FULL:
    ++ProducerStats.local_queue_full;
    LOG(Sev::Warning, "Producer queue full, outq: {}",
        KafkaProducer->outputQueueLength());
    break;

  case RdKafka::ERR_MSG_SIZE_TOO_LARGE:
    ++ProducerStats.msg_too_large;
    LOG(Sev::Error, "Message size too large to publish, size: {}", Msg->size);
    break;

  default:
    ++ProducerStats.produce_fail;
    LOG(Sev::Error, "Publishing message on topic \"{}\" failed",
        RdKafkaTopic->name());
    break;
  }
  return 1;
}

void ProducerTopic::enableCopy() { DoCopyMsg = true; }

std::string ProducerTopic::name() const { return Name; }
}
