// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatbufferReader.h"

namespace FlatbufferMetadata {
using FlatbufferMessage = FileWriter::FlatbufferMessage;
class ev42_Extractor : public FileWriter::FlatbufferReader {
public:
  ev42_Extractor() = default;
  ~ev42_Extractor() = default;
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;

private:
  SharedLogger Logger = spdlog::get("filewriterlogger");
};

} // namespace FlatbufferMetadata
