// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <cstdint>
#include <vector>

namespace Kafka {
/// This class is used for storing messages for sending via librdkafka.
///
/// The producer takes a pointer to the instance and returns it via the
/// callback. It is then manually destructed/deallocated in the callback.
struct ProducerMessage {
  // Virtual to allow overriding in tests.
  virtual ~ProducerMessage() = default;
  std::vector<unsigned char> v;

  [[nodiscard]] std::size_t size() const { return v.size(); }

  [[nodiscard]] unsigned const char *data() const { return v.data(); }
};
} // namespace Kafka
