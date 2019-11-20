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
#include <string>

template <typename T> class JsonMaybe {
public:
  JsonMaybe() = default;
  explicit JsonMaybe(T const &inner) : inner_(inner), found_(true) {}
  explicit operator bool() const { return found_; }
  T inner() const { return inner_; }

private:
  T inner_;
  bool found_ = false;
};

template <typename T>
JsonMaybe<T> find(std::string Key, nlohmann::json const &Json) {
  auto It = Json.find(Key);
  if (It != Json.end()) {
    return JsonMaybe<T>(It.value().get<T>());
  }
  return JsonMaybe<T>();
}
