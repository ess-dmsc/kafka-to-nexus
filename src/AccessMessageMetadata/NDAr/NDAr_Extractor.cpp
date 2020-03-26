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

#include "NDAr_Extractor.h"
#include <NDAr_NDArray_schema_generated.h>

namespace AccessMessageMetadata {

// Register the timestamp and name extraction class for this module.
static FileWriter::FlatbufferReaderRegistry::Registrar<NDAr_Extractor>
    RegisterNDArGuard("NDAr");

bool NDAr_Extractor::verify(FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const std::uint8_t *>(Message.data()), Message.size());
  return FB_Tables::VerifyNDArrayBuffer(Verifier);
}

std::uint64_t epicsTimeToNsec(std::uint64_t sec, std::uint64_t nsec) {
  const auto TimeDiffUNIXtoEPICSepoch = 631152000L;
  const auto NSecMultiplier = 1000000000L;
  return (sec + TimeDiffUNIXtoEPICSepoch) * NSecMultiplier + nsec;
}

uint64_t NDAr_Extractor::timestamp(FlatbufferMessage const &Message) const {
  auto FbPointer = FB_Tables::GetNDArray(Message.data());
  auto epicsTime = FbPointer->epicsTS();
  return epicsTimeToNsec(epicsTime->secPastEpoch(), epicsTime->nsec());
}

std::string
NDAr_Extractor::source_name(const FileWriter::FlatbufferMessage &) const {
  // The source name was left out of the relevant EPICS areaDetector plugin.
  // There is currently a pull request for adding this variable to the FB
  // schema. When the variable has been addded, this function will be updated.
  return "ADPluginKafka";
}
} // namespace AccessMessageMetadata
