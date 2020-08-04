// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <nlohmann/json.hpp>
#include <optional>
#include <string>

template <typename T>
std::optional<T> find(std::string Key, nlohmann::json const &Json) {
  auto It = Json.find(Key);
  if (It != Json.end()) {
    return std::optional<T>(It.value().get<T>());
  }
  return std::nullopt;
}
