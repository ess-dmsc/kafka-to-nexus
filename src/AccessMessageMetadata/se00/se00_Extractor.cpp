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

#include "se00_Extractor.h"
#include <se00_data_generated.h>

namespace AccessMessageMetadata {

// Register the timestamp and name extraction class for this module
static FileWriter::FlatbufferReaderRegistry::Registrar<se00_Extractor>
    RegisterSenvGuard("se00");

bool se00_Extractor::verify(FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const std::uint8_t *>(Message.data()), Message.size());
  return Verifyse00_SampleEnvironmentDataBuffer(Verifier);
}

uint64_t se00_Extractor::timestamp(FlatbufferMessage const &Message) const {
  auto FbPointer = Getse00_SampleEnvironmentData(Message.data());
  return FbPointer->packet_timestamp();
}

std::string se00_Extractor::source_name(
    const FileWriter::FlatbufferMessage &Message) const {
  auto FbPointer = Getse00_SampleEnvironmentData(Message.data());
  return FbPointer->name()->str();
}

} // namespace AccessMessageMetadata
