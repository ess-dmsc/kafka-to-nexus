// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "hs00.h"
#include <hs00_event_histogram_generated.h>

namespace hs00 {

static EventHistogram const *getRoot(char const *Data) {
  return GetEventHistogram(Data);
}

bool hs00::verify(FlatbufferMessage const &Message) const {
  flatbuffers::Verifier Verifier(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyEventHistogramBuffer(Verifier);
}

std::string hs00::source_name(FlatbufferMessage const &Message) const {
  auto Buffer = getRoot(Message.data());
  auto Source = Buffer->source();
  if (Source == nullptr) {
    spdlog::get("filewriterlogger")->info("message has no source_name");
    return "";
  }
  return Source->str();
}

uint64_t hs00::timestamp(FlatbufferMessage const &Message) const {
  auto Buffer = getRoot(Message.data());
  return Buffer->timestamp();
}

FileWriter::FlatbufferReaderRegistry::Registrar<hs00> RegisterReader("hs00");
} // namespace hs00
