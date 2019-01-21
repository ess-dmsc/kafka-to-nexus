#include "BrokerSettings.h"
#include "logger.h"
#include <librdkafka/rdkafka.h>
#include <vector>

namespace KafkaW {

void BrokerSettings::apply(rd_kafka_conf_t *RdKafkaConfiguration) const {
  std::vector<char> ErrorString(256);
  for (const auto &ConfigurationItem : KafkaConfiguration) {
    LOG(Sev::Debug, "set config: {} = {}", ConfigurationItem.first,
        ConfigurationItem.second);
    if (RD_KAFKA_CONF_OK !=
        rd_kafka_conf_set(RdKafkaConfiguration, ConfigurationItem.first.c_str(),
                          ConfigurationItem.second.c_str(), ErrorString.data(),
                          ErrorString.size())) {
      LOG(Sev::Warning, "Failure setting config: {} = {}",
          ConfigurationItem.first, ConfigurationItem.second);
    }
  }
}
} // namespace KafkaW
