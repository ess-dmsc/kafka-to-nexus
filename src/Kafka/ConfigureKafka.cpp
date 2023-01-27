// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <regex>

#include "ConfigureKafka.h"
#include <logger.h>

namespace Kafka {
void configureKafka(RdKafka::Conf *RdKafkaConfiguration,
                    Kafka::BrokerSettings Settings) {
  std::string ErrorString;
  const std::regex RegexSensitiveKey(
      R"(ssl_key|.+password|.+secret|.+key\.pem)");

  for (const auto &ConfigurationItem : Settings.KafkaConfiguration) {
    const std::string &Key{ConfigurationItem.first};
    const std::string &Value{ConfigurationItem.second};
    const bool IsSensitive = std::regex_match(Key, RegexSensitiveKey);

    LOG_DEBUG("Set config: {} = {}", Key, IsSensitive ? "<REDACTED>" : Value);
    if (RdKafka::Conf::ConfResult::CONF_OK !=
        RdKafkaConfiguration->set(Key, Value, ErrorString)) {
      LOG_WARN("Failure setting config: {} = {}", Key,
               IsSensitive ? "<REDACTED>" : Value);
    }
  }
}
} // namespace Kafka
