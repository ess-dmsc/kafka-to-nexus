
#include "NicosCacheReader.h"
#include "ns10_cache_entry_generated.h"

namespace FileWriter {
namespace Schemas {
namespace ns10 {

// Instantiates a ReaderClass used for extracting source names, timestamps and
// verifying a flatbuffers.
static FileWriter::FlatbufferReaderRegistry::Registrar<CacheReader>
    RegisterReader("ns10");

bool CacheReader::verify(FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyCacheEntryBuffer(Verifier);
}

std::string
CacheReader::source_name(FileWriter::FlatbufferMessage const &Message) const {
  auto Entry = GetCacheEntry(Message.data());
  std::string NicosKey = Entry->key()->str();
  return NicosKey;
}

uint64_t
CacheReader::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto Entry = GetCacheEntry(Message.data());
  // NICOS uses (double) seconds for the timestamping. A conversion to ns is
  // required.
  double TimeNs = 1e9 * Entry->time();
  return std::lround(TimeNs);
}

} // namespace ns10
} // namespace Schemas
} // namespace FileWriter
