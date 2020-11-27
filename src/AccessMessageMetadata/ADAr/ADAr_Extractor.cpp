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

#include "ADAr_Extractor.h"
#include <ADAr_area_detector_array_generated.h>

namespace AccessMessageMetadata {

// Register the timestamp and name extraction class for this module.
static FileWriter::FlatbufferReaderRegistry::Registrar<ADAr_Extractor>
    RegisterNDArGuard("ADAr");

bool ADAr_Extractor::verify(FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const std::uint8_t *>(Message.data()), Message.size());
  return VerifyADArrayBuffer(Verifier);
}

uint64_t ADAr_Extractor::timestamp(FlatbufferMessage const &Message) const {
  auto FbPointer = GetADArray(Message.data());
  return FbPointer->timestamp();
}

std::string
ADAr_Extractor::source_name(const FileWriter::FlatbufferMessage &Message) const {
  auto FbPointer = GetADArray(Message.data());
  // The source name was left out of the relevant EPICS areaDetector plugin.
  // There is currently a pull request for adding this variable to the FB
  // schema. When the variable has been addded, this function will be updated.
  return FbPointer->source_name()->str();
}
} // namespace AccessMessageMetadata
