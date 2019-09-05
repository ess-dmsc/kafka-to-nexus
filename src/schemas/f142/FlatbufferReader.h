// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "../../FlatbufferReader.h"
#include "Common.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// Declare helper to extract flatbuffer data from byte blob
FBUF const *get_fbuf(char const *data);

/// Implement Reader interface for f142
class FlatbufferReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;
};
} // namespace f142
} // namespace Schemas
} // namespace FileWriter
