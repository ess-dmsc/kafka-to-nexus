#include "BrokerSettings.h"
#include "logger.h"
#include <librdkafka/rdkafka.h>
#include <vector>

namespace KafkaW {

void BrokerSettings::apply(rd_kafka_conf_t *RdKafkaConfiguration) {
  std::vector<char> ErrorString(256);
  for (auto &c : KafkaConfiguration) {
    LOG(Sev::Debug, "set config: {} = {}", c.first, c.second);
    if (RD_KAFKA_CONF_OK != rd_kafka_conf_set(RdKafkaConfiguration,
                                              c.first.c_str(), c.second.c_str(),
                                              ErrorString.data(),
                                              ErrorString.size())) {
      LOG(Sev::Warning, "error setting config: {} = {}", c.first, c.second);
    }
  }
}
} // namespace KafkaW
