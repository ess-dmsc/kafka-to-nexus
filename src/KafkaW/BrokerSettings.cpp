#include "BrokerSettings.h"
#include "logger.h"
#include <vector>

namespace KafkaW {
void BrokerSettings::apply(RdKafka::Conf *RdKafkaConfiguration) {
  std::string ErrorString;
  for (const auto &ConfigurationItem : KafkaConfiguration) {
    LOG(Sev::Debug, "set config: {} = {}", ConfigurationItem.first,
        ConfigurationItem.second);
    if (RdKafka::Conf::ConfResult::CONF_OK !=
        RdKafkaConfiguration->set(ConfigurationItem.first,
                                  ConfigurationItem.second, ErrorString)) {
      LOG(Sev::Warning, "Failure setting config: {} = {}",
          ConfigurationItem.first, ConfigurationItem.second);
    }
  }
}

} // namespace KafkaW
