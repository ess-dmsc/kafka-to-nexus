#pragma once

#include "logger.h"
#include <librdkafka/rdkafka.h>
#include <map>
#include <string>

namespace KafkaW {

class TopicSettings {
public:
  TopicSettings();
  void applySettingsToRdKafkaConf(rd_kafka_topic_conf_t *conf);
  std::map<std::string, int> ConfigurationIntegers;
  std::map<std::string, std::string> ConfigurationStrings;
};
}
