#include "TopicSettings.h"
#include <vector>

namespace KafkaW {

TopicSettings::TopicSettings() {}

void TopicSettings::applySettingsToRdKafkaConf(rd_kafka_topic_conf_t *conf) {
  std::vector<char> ErrorString(1024);
  for (auto &c : ConfigurationIntegers) {
    std::string ConfigString = fmt::format("{:d}", c.second);
    LOG(spdlog::level::trace, "use  {}: {}", c.first, ConfigString);
    auto err =
        rd_kafka_topic_conf_set(conf, c.first.c_str(), ConfigString.c_str(),
                                ErrorString.data(), ErrorString.size());
    if (err != RD_KAFKA_CONF_OK) {
      LOG(spdlog::level::warn, "Failed to set topic config: {} = {}.  {}",
          c.first, ConfigString, ErrorString.data());
    }
  }
  for (auto &c : ConfigurationStrings) {
    LOG(spdlog::level::trace, "use  {}: {}", c.first, c.second);
    auto err = rd_kafka_topic_conf_set(conf, c.first.c_str(), c.second.c_str(),
                                       ErrorString.data(), ErrorString.size());
    if (err != RD_KAFKA_CONF_OK) {
      LOG(spdlog::level::warn, "Failed to set topic config: {} = {}.  {}",
          c.first, c.second, ErrorString.data());
    }
  }
}
}
