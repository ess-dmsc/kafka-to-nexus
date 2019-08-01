#pragma once

#include "logger.h"
#include <librdkafka/rdkafkacpp.h>
#include <map>
#include <string>

namespace KafkaW {

/// Collect options used to connect to the broker.
struct BrokerSettings {
  BrokerSettings() = default;
  std::string Address;
  int PollTimeoutMS = 100;
  int MetadataTimeoutMS = 2000;
  int OffsetsForTimesTimeoutMS = 2000;
  int ConsumerCloseTimeoutMS = 5000;
  std::map<std::string, std::string> KafkaConfiguration = {
      {"metadata.request.timeout.ms", "2000"}, // 2 Secs
      {"socket.timeout.ms", "2000"},
      {"message.max.bytes", "24117248"}, // 23 MiB
      {"fetch.message.max.bytes", "24117248"},
      {"receive.message.max.bytes", "24117248"},
      {"queue.buffering.max.messages", "100000"},
      {"queue.buffering.max.ms", "50"},
      {"queue.buffering.max.kbytes", "819200"}, // 819.2 Mib
      {"batch.num.messages", "100000"},
      {"coordinator.query.interval.ms", "2000"},
      {"heartbeat.interval.ms", "500"},     // 0.5 Secs
      {"statistics.interval.ms", "600000"}, // 1 Min
      {"api.version.request", "true"},
      {"enable.auto.commit", "false"}};
};
} // namespace KafkaW
