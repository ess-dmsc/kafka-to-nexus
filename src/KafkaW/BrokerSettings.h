#pragma once

#include <map>
#include <string>

struct rd_kafka_conf_s;
typedef struct rd_kafka_conf_s rd_kafka_conf_t;

namespace KafkaW {

/// Collect options used to connect to the broker.

class BrokerSettings {
public:
  BrokerSettings();
  void apply(rd_kafka_conf_t *conf);
  std::string Address;
  size_t PollTimeoutMS = 100;
  std::map<std::string, int> conf_ints;
  std::map<std::string, std::string> conf_strings;
};
}
