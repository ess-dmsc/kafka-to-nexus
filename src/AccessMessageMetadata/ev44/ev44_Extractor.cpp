// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ev44_Extractor.h"
#include <ev44_events_generated.h>

namespace AccessMessageMetadata {

bool ev44_Extractor::verify(FlatbufferMessage const &Message) const {
  flatbuffers::Verifier VerifierInstance(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyEvent44MessageBuffer(VerifierInstance);
}

std::string
ev44_Extractor::source_name(FlatbufferMessage const &Message) const {
  auto fbuf = GetEvent44Message(Message.data());
  auto NamePtr = fbuf->source_name();
  if (NamePtr == nullptr) {
    Logger::Info("message has no source_name");
    return "";
  }
  return NamePtr->str();
}

uint64_t ev44_Extractor::timestamp(FlatbufferMessage const &Message) const {
  auto fbuf = GetEvent44Message(Message.data());
  return fbuf->reference_time()->Get(0);
}

static FileWriter::FlatbufferReaderRegistry::Registrar<ev44_Extractor>
    RegisterReader("ev44");

} // namespace AccessMessageMetadata
