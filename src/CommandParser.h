// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file This file defines the different success and failure status that the
/// `StreamMaster` and the `Streamer` can incur. These error object have some
/// utility methods that can be used to test the more common situations.

#pragma once

#include "URI.h"
#include "json.h"
#include "logger.h"
#include <chrono>
#include <iostream>

namespace FileWriter {

struct StartCommandInfo {
  std::string JobID;
  std::string Filename;
  std::string NexusStructure;
  std::string ServiceID;
  bool UseSwmr = true;
  bool AbortOnStreamFailure = false;
  uri::URI BrokerInfo{"localhost:9092"};
  std::chrono::milliseconds StartTime{0};
  std::chrono::milliseconds StopTime{0};
};

class CommandParser {
public:
  StartCommandInfo extractStartInformation(
      const nlohmann::json &JSONCommand,
      std::chrono::milliseconds DefaultStartTime = getCurrentTime());

private:
  SharedLogger Logger = getLogger();
  void extractBroker(nlohmann::json const &JSONCommand, uri::URI &BrokerInfo);
  void extractJobID(nlohmann::json const &JSONCommand, std::string & JobID);
  static std::chrono::duration<long long int, std::milli> getCurrentTime();
  std::chrono::milliseconds
  extractTime(std::string const &Key, nlohmann::json const &JSONCommand,
              std::chrono::milliseconds const &DefaultTime);

  template <typename T>
  T getRequiredValue(std::string const &Key,
                     nlohmann::json const &JSONCommand) {
    if (auto x = find<T>(Key, JSONCommand)) {
      return x.inner();
    }

    throw std::runtime_error(
        fmt::format("Missing key {} from command JSON", Key));
  }

  template <typename T>
  void setOptionalValue(std::string const &Key,
                        nlohmann::json const &JSONCommand, T &Value) {
    if (auto x = find<T>(Key, JSONCommand)) {
      Value = x.inner();
    }
  }
};
} // namespace FileWriter
