#include "BrokerSettings.h"
#include "logger.h"
#include <vector>

namespace KafkaW {
void BrokerSettings::apply(RdKafka::Conf *RdKafkaConfiguration) const {
  std::shared_ptr<spdlog::logger> Logger = spdlog::get("filewriterlogger");
  std::string ErrorString;
  for (const auto &ConfigurationItem : KafkaConfiguration) {
    Logger->debug("set config: {} = {}", ConfigurationItem.first,
                  ConfigurationItem.second);
    if (RdKafka::Conf::ConfResult::CONF_OK !=
        RdKafkaConfiguration->set(ConfigurationItem.first,
                                  ConfigurationItem.second, ErrorString)) {
      Logger->warn("Failure setting config: {} = {}", ConfigurationItem.first,
                   ConfigurationItem.second);
    }
  }
}

} // namespace KafkaW
