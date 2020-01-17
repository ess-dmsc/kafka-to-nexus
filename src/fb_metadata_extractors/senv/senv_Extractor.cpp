// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
/// \brief Implement classes required to implement the ADC file writing module.

#include "senv_Extractor.h"
#include <senv_data_generated.h>

namespace FlatbufferMetadata {

// Register the timestamp and name extraction class for this module
static FileWriter::FlatbufferReaderRegistry::Registrar<senv_Extractor>
    RegisterSenvGuard("senv");

bool senv_Extractor::verify(FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const std::uint8_t *>(Message.data()), Message.size());
  return VerifySampleEnvironmentDataBuffer(Verifier);
}

uint64_t senv_Extractor::timestamp(FlatbufferMessage const &Message) const {
  auto FbPointer = GetSampleEnvironmentData(Message.data());
  return FbPointer->PacketTimestamp();
}

std::string senv_Extractor::source_name(
    const FileWriter::FlatbufferMessage &Message) const {
  auto FbPointer = GetSampleEnvironmentData(Message.data());
  return FbPointer->Name()->str();
}

} // namespace FlatbufferMetadata
