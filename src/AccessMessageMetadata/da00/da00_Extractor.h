// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "FlatbufferReader.h"

namespace AccessMessageMetadata {
using FBMessage = FileWriter::FlatbufferMessage;
using FBReaderBase = FileWriter::FlatbufferReader;

/// \brief For extracting info from da00 flatbuffer messages.
class da00_Extractor : public FBReaderBase {
public:
  [[nodiscard]] bool verify(FBMessage const &Message) const override;
  [[nodiscard]] std::string source_name(FBMessage const &) const override;
  [[nodiscard]] uint64_t timestamp(FBMessage const &Message) const override;
};
} // namespace AccessMessageMetadata