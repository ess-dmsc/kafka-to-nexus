// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include <stdint.h>

namespace KafkaW {
struct ProducerMessage {
  virtual ~ProducerMessage() = default;
  unsigned char *data;
  uint32_t size;
};
} // namespace KafkaW
