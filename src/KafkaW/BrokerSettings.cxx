#include "BrokerSettings.h"
#include "logger.h"
#include <librdkafka/rdkafka.h>
#include <vector>

namespace KafkaW {

BrokerSettings::BrokerSettings() {
  ConfigurationIntegers = {
      {"metadata.request.timeout.ms", 2 * 1000},
      {"socket.timeout.ms", 2 * 1000},
      {"message.max.bytes", 23 * 1024 * 1024},
      {"fetch.message.max.bytes", 23 * 1024 * 1024},
      {"receive.message.max.bytes", 23 * 1024 * 1024},
      {"queue.buffering.max.messages", 100 * 1000},
      {"queue.buffering.max.ms", 50},
      {"queue.buffering.max.kbytes", 800 * 1024},
      {"batch.num.messages", 100 * 1000},
      {"coordinator.query.interval.ms", 2 * 1000},
      {"heartbeat.interval.ms", 500},
      {"statistics.interval.ms", 600 * 1000},
  };
  ConfigurationStrings = {
      {"api.version.request", "true"},
  };
}

void BrokerSettings::apply(rd_kafka_conf_t *RdKafkaConfiguration) {
  std::vector<char> ErrorString(256);
  for (auto &c : ConfigurationIntegers) {
    auto s1 = fmt::format("{:d}", c.second);
    LOG(Sev::Debug, "set config: {} = {}", c.first, s1);
    if (RD_KAFKA_CONF_OK !=
        rd_kafka_conf_set(RdKafkaConfiguration, c.first.c_str(), s1.c_str(),
                          ErrorString.data(), ErrorString.size())) {
      LOG(Sev::Warning, "error setting config: {} = {}", c.first, s1);
    }
  }
  for (auto &c : ConfigurationStrings) {
    LOG(Sev::Debug, "set config: {} = {}", c.first, c.second);
    if (RD_KAFKA_CONF_OK != rd_kafka_conf_set(RdKafkaConfiguration,
                                              c.first.c_str(), c.second.c_str(),
                                              ErrorString.data(),
                                              ErrorString.size())) {
      LOG(Sev::Warning, "error setting config: {} = {}", c.first, c.second);
    }
  }
}
}
