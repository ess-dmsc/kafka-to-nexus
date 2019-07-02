#pragma once
#include "logger.h"
#include <librdkafka/rdkafkacpp.h>

namespace KafkaW {
class KafkaEventCb : public RdKafka::EventCb {
public:
  void event_cb(RdKafka::Event &Event) override {
    switch (Event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      Logger->log(spdlog::level::level_enum(LogLevels.at(Event.severity())),
                  "Kafka EVENT_ERROR id: {}  broker: {}  errorname: {}  "
                  "errorstring: {}",
                  Event.broker_id(), Event.broker_name().c_str(),
                  RdKafka::err2str(Event.err()), Event.str());
      break;
    case RdKafka::Event::EVENT_STATS:
      Logger->log(spdlog::level::level_enum(LogLevels.at(Event.severity())),
                  "Kafka Stats id: {} broker: {} message: {}",
                  Event.broker_id(), Event.broker_name(), Event.str());
      break;
    case RdKafka::Event::EVENT_LOG:
      Logger->log(
          spdlog::level::level_enum(LogLevels.at(Event.severity())),
          "Kafka Log id: {} broker: {} severity: {}, facilitystr: {}:{}",
          Event.broker_id(), Event.broker_name(), Event.severity(), Event.fac(),
          Event.str());
      break;
    default:
      Logger->log(spdlog::level::level_enum(LogLevels.at(Event.severity())),
                  "Kafka Event {} ({}): {}", Event.type(),
                  RdKafka::err2str(Event.err()), Event.str());
      break;
    }
  };

private:
  std::map<RdKafka::Event::Severity, int> LogLevels{
      {RdKafka::Event::Severity::EVENT_SEVERITY_DEBUG, SPDLOG_LEVEL_TRACE},
      {RdKafka::Event::Severity::EVENT_SEVERITY_INFO, SPDLOG_LEVEL_DEBUG},
      {RdKafka::Event::Severity::EVENT_SEVERITY_NOTICE, SPDLOG_LEVEL_INFO},
      {RdKafka::Event::Severity::EVENT_SEVERITY_WARNING, SPDLOG_LEVEL_WARN},
      {RdKafka::Event::Severity::EVENT_SEVERITY_ERROR, SPDLOG_LEVEL_ERROR},
      {RdKafka::Event::Severity::EVENT_SEVERITY_CRITICAL,
       SPDLOG_LEVEL_CRITICAL},
      {RdKafka::Event::Severity::EVENT_SEVERITY_ALERT, SPDLOG_LEVEL_CRITICAL},
      {RdKafka::Event::Severity::EVENT_SEVERITY_EMERG, SPDLOG_LEVEL_CRITICAL}};
  SharedLogger Logger = spdlog::get("filewriterlogger");
};
} // namespace KafkaW
