// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include "logger.h"
#include <librdkafka/rdkafkacpp.h>

namespace Kafka {
class KafkaEventCb : public RdKafka::EventCb {
public:
  void event_cb(RdKafka::Event &Event) override {
    switch (Event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      Logger::Log(LogLevels.at(Event.severity()), "Kafka EVENT_ERROR {} [{}]",
                  RdKafka::err2str(Event.err()), Event.str());
      break;
    case RdKafka::Event::EVENT_STATS:
      // Logger::Log(LogLevels.at(Event.severity()),
      //             "Kafka Stats id: {} broker: {} message: {}",
      //             Event.broker_id(), Event.broker_name(), Event.str());
      break;
    case RdKafka::Event::EVENT_LOG:
      if (std::string(Event.fac()).find("CONFWARN") != std::string::npos) {
        // Skip some configuration warnings entirely
        if (std::string(Event.str())
                    .find("is a producer property and will be ignored by this "
                          "consumer") != std::string::npos ||
            std::string(Event.str())
                    .find("is a consumer property and will be ignored by this "
                          "producer") != std::string::npos) {
          break;
        }
        // Override severity of the remaining CONFWARN messages
        Logger::Log(static_cast<int>(LogSeverity::Debug), "Kafka Log {} {}",
                    Event.fac(), Event.str());
      } else {
        Logger::Log(LogLevels.at(Event.severity()), "Kafka Log {} {}",
                    Event.fac(), Event.str());
      }
      break;
    default:
      Logger::Log(LogLevels.at(Event.severity()), "Kafka Event {} ({}): {}",
                  Event.type(), RdKafka::err2str(Event.err()), Event.str());

      break;
    }
  }

private:
  std::map<RdKafka::Event::Severity, int> LogLevels{
      {RdKafka::Event::Severity::EVENT_SEVERITY_DEBUG,
       static_cast<int>(LogSeverity::Debug)},
      {RdKafka::Event::Severity::EVENT_SEVERITY_INFO,
       static_cast<int>(LogSeverity::Info)},
      {RdKafka::Event::Severity::EVENT_SEVERITY_NOTICE,
       static_cast<int>(LogSeverity::Info)},
      {RdKafka::Event::Severity::EVENT_SEVERITY_WARNING,
       static_cast<int>(LogSeverity::Warn)},
      {RdKafka::Event::Severity::EVENT_SEVERITY_ERROR,
       static_cast<int>(LogSeverity::Error)},
      {RdKafka::Event::Severity::EVENT_SEVERITY_CRITICAL,
       static_cast<int>(LogSeverity::Critical)},
      {RdKafka::Event::Severity::EVENT_SEVERITY_ALERT,
       static_cast<int>(LogSeverity::Critical)},
      {RdKafka::Event::Severity::EVENT_SEVERITY_EMERG,
       static_cast<int>(LogSeverity::Critical)}};
};
} // namespace Kafka
