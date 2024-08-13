// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ad00_Extractor.h"
#include <ad00_area_detector_array_generated.h>

namespace AccessMessageMetadata {

// Register the timestamp and name extraction class for this module.
static FileWriter::FlatbufferReaderRegistry::Registrar<ad00_Extractor>
    RegisterNDArGuard("ad00");

bool ad00_Extractor::verify(FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const std::uint8_t *>(Message.data()), Message.size());
  return Verifyad00_ADArrayBuffer(Verifier);
}

uint64_t ad00_Extractor::timestamp(FlatbufferMessage const &Message) const {
  auto FbPointer = Getad00_ADArray(Message.data());
  return FbPointer->timestamp();
}

std::string ad00_Extractor::source_name(
    const FileWriter::FlatbufferMessage &Message) const {
  auto FbPointer = Getad00_ADArray(Message.data());
  // The source name was left out of the relevant EPICS areaDetector plugin.
  // There is currently a pull request for adding this variable to the FB
  // schema. When the variable has been addded, this function will be updated.
  return FbPointer->source_name()->str();
}
} // namespace AccessMessageMetadata
