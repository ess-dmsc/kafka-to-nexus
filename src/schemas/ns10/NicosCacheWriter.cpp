
#include "NicosCacheWriter.h"
#include "ns10_cache_entry_generated.h"
#include <regex>

namespace NicosCacheWriter {

// Instantiates a ReaderClass used for extracting source names, timestamps and
// verifying a flatbuffers.
static FileWriter::FlatbufferReaderRegistry::Registrar<ReaderClass>
    RegisterReader("ns10");

// Creates a factory function used to instantiate zero or more WriterClass, i.e.
// one for every data source which produces data with the file id "test".
static FileWriter::HDFWriterModuleRegistry::Registrar<WriterClass>
    RegisterWriter("ns10");

} // namespace NicosCacheWriter

namespace NicosCacheWriter {

bool ReaderClass::verify(FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyCacheEntryBuffer(Verifier);
}

std::string
ReaderClass::source_name(FileWriter::FlatbufferMessage const &Message) const {
  auto Entry = GetCacheEntry(Message.data());
  std::string NicosKey(Entry->key()->str());
  std::smatch Matches;
  std::regex Regex(R"((.*)\/(.*)\/(.*))");
  std::regex_match(NicosKey, Matches, Regex);

  if (Matches.size() < 4) {
    throw std::runtime_error(
        "Unable to extract NICOS key information from: \"" + NicosKey);
  }
  std::string Source = Matches[1].str();
  std::string Device = Matches[2].str();
  std::string Parameter = Matches[3].str();
  return Source;
}

uint64_t
ReaderClass::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto Entry = GetCacheEntry(Message.data());
  // NICOS uses (double) seconds fot the timestamping. A conversion to ns is
  // required.
  double TimeNs = 1e9 * Entry->time();
  return std::lround(TimeNs);
}

} // namespace NicosCacheWriter
