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

namespace KafkaW {
void configureKafka(RdKafka::Conf *RdKafkaConfiguration,
                    KafkaW::BrokerSettings Settings) {
  SharedLogger Logger = spdlog::get("filewriterlogger");
  std::string ErrorString;
  for (const auto &ConfigurationItem : Settings.KafkaConfiguration) {
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
