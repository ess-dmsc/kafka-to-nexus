// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "hs00_Extractor.h"
#include <hs00_event_histogram_generated.h>

namespace AccessMessageMetadata {
bool hs00_Extractor::verify(FlatbufferMessage const &Message) const {
  flatbuffers::Verifier Verifier(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyEventHistogramBuffer(Verifier);
}

std::string
hs00_Extractor::source_name(FlatbufferMessage const &Message) const {
  auto Buffer = GetEventHistogram(Message.data());
  auto Source = Buffer->source();
  if (Source == nullptr) {
    Logger::Info("message has no source_name");
    return "";
  }
  return Source->str();
}

uint64_t hs00_Extractor::timestamp(FlatbufferMessage const &Message) const {
  auto Buffer = GetEventHistogram(Message.data());
  return Buffer->timestamp();
}

FileWriter::FlatbufferReaderRegistry::Registrar<hs00_Extractor>
    RegisterReader("hs00");
} // namespace AccessMessageMetadata
