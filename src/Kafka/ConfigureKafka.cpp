// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ConfigureKafka.h"
#include <logger.h>

namespace Kafka {
void configureKafka(RdKafka::Conf *RdKafkaConfiguration,
                    Kafka::BrokerSettings Settings) {
  std::string ErrorString;
  for (const auto &ConfigurationItem : Settings.KafkaConfiguration) {
    std::string Key{ConfigurationItem.first};
    std::string Value{ConfigurationItem.second};

    // Don't log sensitive data
    if (Key == "sasl.password") {
      Value = "<REDACTED>";
    }

    LOG_DEBUG("set config: {} = {}", Key, Value);
    if (RdKafka::Conf::ConfResult::CONF_OK !=
        RdKafkaConfiguration->set(Key, Value, ErrorString)) {
      LOG_WARN("Failure setting config: {} = {}", Key, Value);
    }
  }
}
} // namespace Kafka
