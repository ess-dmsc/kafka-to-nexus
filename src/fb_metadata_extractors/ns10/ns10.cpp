// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ns10.h"
#include <ns10_cache_entry_generated.h>

namespace ns10 {

// Instantiates a ReaderClass used for extracting source names, timestamps and
// verifying a flatbuffers.
static FileWriter::FlatbufferReaderRegistry::Registrar<ns10>
    RegisterReader("ns10");

bool ns10::verify(FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyCacheEntryBuffer(Verifier);
}

std::string
ns10::source_name(FileWriter::FlatbufferMessage const &Message) const {
  auto Entry = GetCacheEntry(Message.data());
  std::string NicosKey = Entry->key()->str();
  return NicosKey;
}

uint64_t
ns10::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto Entry = GetCacheEntry(Message.data());
  // NICOS uses (double) seconds for the timestamping. A conversion to ns is
  // required.
  double TimeNs = 1e9 * Entry->time();
  return std::lround(TimeNs);
}

} // namespace ns10
