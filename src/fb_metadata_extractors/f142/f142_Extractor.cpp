// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "f142_Extractor.h"
#include <f142_logdata_generated.h>

namespace FlatbufferMetadata {
/// \brief  Use flatbuffers library to validate a message
bool f142_Extractor::verify(FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyLogDataBuffer(Verifier);
}

/// Extract name of source from the message
std::string
f142_Extractor::source_name(FlatbufferMessage const &Message) const {
  auto LogDataBuffer = GetLogData(Message.data());
  auto s1 = LogDataBuffer->source_name();
  if (s1 == nullptr) {
    spdlog::get("filewriterlogger")->warn("message has no source name");
    return "";
  }
  return s1->str();
}

/// Extract timestamp from the message
uint64_t f142_Extractor::timestamp(FlatbufferMessage const &Message) const {
  auto LogDataBuffer = GetLogData(Message.data());
  return LogDataBuffer->timestamp();
}

/// Register the Reader with the application's registry
static FileWriter::FlatbufferReaderRegistry::Registrar<f142_Extractor>
    RegisterReader("f142");
} // namespace FlatbufferMetadata
