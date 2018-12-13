#pragma once
#include "logger.h"
#include <functional>
#include <librdkafka/rdkafkacpp.h>

namespace KafkaW {
class ProducerInterface;

class ProducerEventCb : public RdKafka::EventCb {
public:
  void event_cb(RdKafka::Event &event) override {
    switch (event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      LOG(event.severity(),
          "Kafka EVENT_ERROR id: {}  broker: {}  errno: {}  errorname: {}  "
          "errorstring: {}",
          event.broker_id(), event.broker_name(), event.type(),
          RdKafka::err2str(event.err()), event.str());
      break;

    case RdKafka::Event::EVENT_STATS:
      LOG(event.severity(), "Kafka Stats id: {} broker: {} message: {}",
          event.broker_id(), event.broker_name(), event.str());
      break;

    case RdKafka::Event::EVENT_LOG:
      LOG(event.severity(),
          "Kafka Log id: {} broker: {} severity: {}, facilitystr: {}:{}",
          event.broker_id(), event.broker_name(), event.severity(), event.fac(),
          event.str());
      break;

    default:
      LOG(event.severity(), "Kafka Event {} ({}): {}", event.type(),
          RdKafka::err2str(event.err()), event.str());
      break;
    }
  };

private:
};
}