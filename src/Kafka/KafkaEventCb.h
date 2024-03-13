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
      Log::FmtMsg(LogLevels.at(Event.severity()), "Kafka EVENT_ERROR {} [{}]",
                  RdKafka::err2str(Event.err()), Event.str());
      break;
    case RdKafka::Event::EVENT_STATS:
      // Log::FmtMsg(LogLevels.at(Event.severity()),
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
        Log::FmtMsg(Log::Severity::Debug, "Kafka Log {} {}", Event.fac(),
                    Event.str());
      } else {
        Log::FmtMsg(LogLevels.at(Event.severity()), "Kafka Log {} {}",
                    Event.fac(), Event.str());
      }
      break;
    default:
      Log::FmtMsg(LogLevels.at(Event.severity()), "Kafka Event {} ({}): {}",
                  Event.type(), RdKafka::err2str(Event.err()), Event.str());
      break;
    }
  }

private:
  std::map<RdKafka::Event::Severity, Log::Severity> LogLevels{
      {RdKafka::Event::Severity::EVENT_SEVERITY_DEBUG, Log::Severity::Debug},
      {RdKafka::Event::Severity::EVENT_SEVERITY_INFO, Log::Severity::Info},
      {RdKafka::Event::Severity::EVENT_SEVERITY_NOTICE, Log::Severity::Notice},
      {RdKafka::Event::Severity::EVENT_SEVERITY_WARNING,
       Log::Severity::Warning},
      {RdKafka::Event::Severity::EVENT_SEVERITY_ERROR, Log::Severity::Error},
      {RdKafka::Event::Severity::EVENT_SEVERITY_CRITICAL,
       Log::Severity::Critical},
      {RdKafka::Event::Severity::EVENT_SEVERITY_ALERT, Log::Severity::Critical},
      {RdKafka::Event::Severity::EVENT_SEVERITY_EMERG,
       Log::Severity::Critical}};
};
} // namespace Kafka
