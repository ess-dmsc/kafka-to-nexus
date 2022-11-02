// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatbufferReader.h"

namespace AccessMessageMetadata {
using FlatbufferMessage = FileWriter::FlatbufferMessage;

/// \brief For extracting info from ev44 flatbuffer messages.
class ev44_Extractor : public FileWriter::FlatbufferSignedIntegersReader {
public:
  ev44_Extractor() = default;
  ~ev44_Extractor() = default;
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  const flatbuffers::Vector<int64_t> *
  timestamp(FlatbufferMessage const &Message) const override;
};

} // namespace AccessMessageMetadata
