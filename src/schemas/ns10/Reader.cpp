
#include "NicosCacheWriter.h"
#include "ns10_cache_entry_generated.h"

namespace NicosCacheWriter {

// Instantiates a ReaderClass used for extracting source names, timestamps and
// verifying a flatbuffers.
static FileWriter::FlatbufferReaderRegistry::Registrar<CacheReader>
    RegisterReader("ns10");
}

namespace NicosCacheWriter {

bool CacheReader::verify(FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyCacheEntryBuffer(Verifier);
}

bool key_is_valid(const std::string &CacheKey) {
  if (std::count(CacheKey.begin(), CacheKey.end(), '/') == 2) {
    return true;
  }
  return false;
}

std::tuple<std::string, std::string, std::string> CacheReader::parse_nicos_key(
    FileWriter::FlatbufferMessage const &Message) const {
  auto Entry = GetCacheEntry(Message.data());
  std::string NicosKey{Entry->key()->str()};
  if (!key_is_valid(NicosKey)) {
    throw std::runtime_error(
        "Unable to extract NICOS key information from: \"" + NicosKey);
  }
  auto From = NicosKey.find_first_of("/");
  auto To = NicosKey.find_last_of("/");
  if (From == 0 || From == To - 1 || To + 1 == NicosKey.length()) {
    throw std::runtime_error("Malformed NICOS key: \"" + NicosKey);
  }

  return std::make_tuple(NicosKey.substr(0, From),
                         NicosKey.substr(From + 1, To - From - 1),
                         NicosKey.substr(To + 1));
}

std::string
CacheReader::source_name(FileWriter::FlatbufferMessage const &Message) const {
  auto Values = parse_nicos_key(Message);
  return std::get<0>(Values);
}

std::string
CacheReader::device_name(FileWriter::FlatbufferMessage const &Message) const {
  auto Values = parse_nicos_key(Message);
  return std::get<1>(Values);
}

std::string CacheReader::parameter_name(
    FileWriter::FlatbufferMessage const &Message) const {
  auto Values = parse_nicos_key(Message);
  return std::get<2>(Values);
}

uint64_t
CacheReader::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto Entry = GetCacheEntry(Message.data());
  // NICOS uses (double) seconds for the timestamping. A conversion to ns is
  // required.
  double TimeNs = 1e9 * Entry->time();
  return std::lround(TimeNs);
}

} // namespace
