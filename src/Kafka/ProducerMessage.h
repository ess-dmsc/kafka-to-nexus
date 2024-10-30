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
/// This class is used for storing messages before passing them on to librdkafka.
/// The lifetime of this object MUST exceed the call to librdkafka's producer.
/// I.e. it must not be deleted until it has been sent to Kafka.
struct ProducerMessage {
  // TODO: do we need to inheirt from this?
  virtual ~ProducerMessage() = default;
  std::vector<unsigned char> v;

  [[nodiscard]] size_t size() const {
    return v.size();
  }

  [[nodiscard]] unsigned const char * data() const {
    return v.data();
  }

};
} // namespace Kafka
