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
using FlatbufferMessage = FileWriter::FlatbufferMessage;

/// \brief For extracting info from f142 flatbuffer messages.
class f142_Extractor : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;
};
} // namespace AccessMessageMetadata
