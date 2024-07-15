// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ev42_Extractor.h"
#include <ev42_events_generated.h>

namespace AccessMessageMetadata {

bool ev42_Extractor::verify(FlatbufferMessage const &Message) const {
  flatbuffers::Verifier VerifierInstance(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyEventMessageBuffer(VerifierInstance);
}

std::string
ev42_Extractor::source_name(FlatbufferMessage const &Message) const {
  auto fbuf = GetEventMessage(Message.data());
  auto NamePtr = fbuf->source_name();
  if (NamePtr == nullptr) {
    Logger::Info("message has no source_name");
    return "";
  }
  return NamePtr->str();
}

uint64_t ev42_Extractor::timestamp(FlatbufferMessage const &Message) const {
  auto fbuf = GetEventMessage(Message.data());
  return fbuf->pulse_time();
}

static FileWriter::FlatbufferReaderRegistry::Registrar<ev42_Extractor>
    RegisterReader("ev42");

} // namespace AccessMessageMetadata
