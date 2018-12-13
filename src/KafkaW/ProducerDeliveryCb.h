#pragma once

#include "Producer.h"
#include "logger.h"
#include <librdkafka/rdkafkacpp.h>
#include <utility>

namespace KafkaW {

class ProducerDeliveryCb : public RdKafka::DeliveryReportCb {
public:
  explicit ProducerDeliveryCb(std::shared_ptr<Producer> Producer)
      : Prod(std::move(Producer)){};
  void dr_cb(RdKafka::Message &Message) override {
    if (Message.err()) {
      LOG(Sev::Error, "ERROR on delivery, {}, topic {}, {} [{}] {}",
          Prod->getRdKafkaPtr()->name(), Message.topic_name(), Message.err(),
          Message.errstr(), RdKafka::err2str(Message.err()));
      if (Prod->on_delivery_failed) {
        Prod->on_delivery_failed(&Message);
      }
      ++Prod->Stats.produce_cb_fail;
    } else {
      if (Prod->on_delivery_ok) {
        Prod->on_delivery_ok(&Message);
      }
      ++Prod->Stats.produce_cb;
    }
  }

private:
  std::shared_ptr<Producer> Prod;
};
}
