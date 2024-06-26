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
void configureKafka(RdKafka::Conf &RdKafkaConfiguration,
                    Kafka::BrokerSettings const &Settings) {
  std::string ErrorString;
  const std::regex RegexSensitiveKey(
      R"(ssl_key|.+password|.+secret|.+key\.pem)");
  std::string DebugOutput;

  for (const auto &[Key, Value] : Settings.KafkaConfiguration) {
    const auto IsSensitive = std::regex_match(Key, RegexSensitiveKey);
    const auto LogValue = IsSensitive ? "<REDACTED>" : Value;

    if (RdKafka::Conf::ConfResult::CONF_OK !=
        RdKafkaConfiguration.set(Key, Value, ErrorString)) {
      LOG_WARN("Failure setting config: {} = {}", Key, LogValue);
    } else {
      DebugOutput += fmt::format(" {}={}", Key, LogValue);
    }
  }

  LOG_DEBUG("RdKafka settings applied:{}", DebugOutput);
}

} // namespace Kafka
