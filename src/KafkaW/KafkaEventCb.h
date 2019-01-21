#pragma once
#include "logger.h"
#include <librdkafka/rdkafkacpp.h>

namespace KafkaW {
    class KafkaEventCb : public RdKafka::EventCb {
    public:
        void event_cb(RdKafka::Event &Event) override {
            switch (Event.type()) {
                case RdKafka::Event::EVENT_ERROR:
                LOG(Event.severity(),
                    "Kafka EVENT_ERROR id: {}  broker: {}  errno: {}  errorname: {}  "
                    "errorstring: {}",
                    Event.broker_id(), Event.broker_name(), Event.type(),
                    RdKafka::err2str(Event.err()), Event.str());
                    break;
                case RdKafka::Event::EVENT_STATS:
                LOG(Event.severity(), "Kafka Stats id: {} broker: {} message: {}",
                    Event.broker_id(), Event.broker_name(), Event.str());
                    break;
                case RdKafka::Event::EVENT_LOG:
                LOG(Event.severity(),
                    "Kafka Log id: {} broker: {} severity: {}, facilitystr: {}:{}",
                    Event.broker_id(), Event.broker_name(), Event.severity(), Event.fac(),
                    Event.str());
                    break;
                default:
                LOG(Event.severity(), "Kafka Event {} ({}): {}", Event.type(),
                    RdKafka::err2str(Event.err()), Event.str());
                    break;
            }
        };
    };
}