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
#include "../../HDFWriterModule.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

class Reader : public FileWriter::FlatbufferReader {
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;
};
} // namespace hs00
} // namespace Schemas
} // namespace FileWriter
