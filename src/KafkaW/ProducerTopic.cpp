// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ProducerTopic.h"
#include "ProducerMessage.h"
#include <vector>

namespace KafkaW {

ProducerTopic::ProducerTopic(std::shared_ptr<Producer> ProducerPtr,
                             std::string TopicName)
    : KafkaProducer(ProducerPtr), Name(std::move(TopicName)) {

  std::string ErrStr;
  RdKafkaTopic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(
      KafkaProducer->getRdKafkaPtr(), Name, ConfigPtr.get(), ErrStr));
  if (RdKafkaTopic == nullptr) {
    Logger->error("could not create Kafka topic: {}", ErrStr);
    throw TopicCreationError();
  }
  Logger->debug("Constructor producer topic: {}", RdKafkaTopic->name());
}

struct Msg_ : public ProducerMessage {
  std::vector<unsigned char> v;
  void finalize() {
    data = v.data();
    size = v.size();
  }
};

int ProducerTopic::produce(const std::string &MsgData) {
  auto MsgPtr = new Msg_;
  std::copy(MsgData.begin(), MsgData.end(), std::back_inserter(MsgPtr->v));
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
  auto MsgSize = static_cast<uint64_t>(Msg->size);

  switch (KafkaProducer->produce(
      RdKafkaTopic.get(), RdKafka::Topic::PARTITION_UA, MsgFlags, Msg->data,
      Msg->size, key, key_len, Msg.get())) {
  case RdKafka::ERR_NO_ERROR:
    ++ProducerStats.produced;
    ProducerStats.produced_bytes += MsgSize;
    ++KafkaProducer->TotalMessagesProduced;
    Msg.release(); // we clean up the message after it has been sent, see
                   // comment by MsgFlags declaration
    return 0;

  case RdKafka::ERR__QUEUE_FULL:
    ++ProducerStats.local_queue_full;
    Logger->warn("Producer queue full, outq: {}",
                 KafkaProducer->outputQueueLength());
    break;

  case RdKafka::ERR_MSG_SIZE_TOO_LARGE:
    ++ProducerStats.msg_too_large;
    Logger->error("Message size too large to publish, size: {}", Msg->size);
    break;

  default:
    ++ProducerStats.produce_fail;
    Logger->error("Publishing message on topic \"{}\" failed",
                  RdKafkaTopic->name());
    break;
  }
  return 1;
}

std::string ProducerTopic::name() const { return Name; }
} // namespace KafkaW
