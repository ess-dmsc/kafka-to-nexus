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

#include "da00_Extractor.h"
#include <da00_dataarray_generated.h>

namespace AccessMessageMetadata {

// Register the timestamp and name extraction class for this module.
static FileWriter::FlatbufferReaderRegistry::Registrar<da00_Extractor>
    Register_da00_Guard("da00");

bool da00_Extractor::verify(FBMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const std::uint8_t *>(Message.data()), Message.size());
  return Verifyda00_DataArrayBuffer(Verifier);
}

uint64_t da00_Extractor::timestamp(FBMessage const &Message) const {
  auto FbPointer = Getda00_DataArray(Message.data());
  return FbPointer->timestamp();
}

std::string da00_Extractor::source_name(
    const FileWriter::FlatbufferMessage &Message) const {
  auto FbPointer = Getda00_DataArray(Message.data());
  return FbPointer->source_name()->str();
}
} // namespace AccessMessageMetadata
