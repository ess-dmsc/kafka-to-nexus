#pragma once

#include "SettingsInterface.h"
#include "logger.h"
#include <librdkafka/rdkafka.h>

namespace KafkaW {

struct TopicSettings : public KafkaW::SettingsInterface {
  TopicSettings();
  virtual void apply() override{};
  void apply(rd_kafka_topic_conf_t *conf);
  std::map<std::string, int> ConfigurationIntegers;
};
}
