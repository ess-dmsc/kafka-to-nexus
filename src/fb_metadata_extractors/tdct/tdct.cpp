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
/// \brief Implement classes required for writing chopper time stamps.

#include "tdct.h"
#include <tdct_timestamps_generated.h>

namespace tdct {

// Register the timestamp and name extraction class for this module
static FileWriter::FlatbufferReaderRegistry::Registrar<tdct>
    RegisterSenvGuard("tdct");

bool tdct::verify(FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const std::uint8_t *>(Message.data()), Message.size());
  return VerifytimestampBuffer(Verifier);
}

uint64_t
tdct::timestamp(FlatbufferMessage const &Message) const {
  auto FbPointer = Gettimestamp(Message.data());
  if (FbPointer->timestamps()->size() == 0) {
    throw std::runtime_error(
        "Can not extract timestamp when timestamp array has zero elements.");
  }
  return FbPointer->timestamps()->operator[](0);
}

std::string tdct::source_name(
    const FileWriter::FlatbufferMessage &Message) const {
  auto FbPointer = Gettimestamp(Message.data());
  return FbPointer->name()->str();
}

} // namespace tdct
