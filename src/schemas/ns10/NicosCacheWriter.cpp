
#include "NicosCacheWriter.h"
#include "../../HDFFile.h"
#include "ns10_cache_entry_generated.h"
#include <regex>

namespace NicosCacheWriter {

// Instantiates a ReaderClass used for extracting source names, timestamps and
// verifying a flatbuffers.
static FileWriter::FlatbufferReaderRegistry::Registrar<CacheReader>
    RegisterReader("ns10");

// Creates a factory function used to instantiate zero or more CacheWriter,
// i.e.
// one for every data source which produces data with the file id "test".
static FileWriter::HDFWriterModuleRegistry::Registrar<CacheWriter>
    RegisterWriter("ns10");

} // namespace NicosCacheWriter

namespace NicosCacheWriter {

// void createHDFStructure(hdf5::node::Group &, size_t);

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

////////////////////////////////////////////////
////////////////////////////////////////////////

namespace dataset {
class Value : public NeXusDataset::ExtensibleDataset<const char> {
public:
  Value() = default;
  /// \brief Create the time dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  Value(hdf5::node::Group const &Parent, NeXusDataset::Mode CMode,
        size_t ChunkSize = 1024);
};
Value::Value(hdf5::node::Group const &Parent, NeXusDataset::Mode CMode,
             size_t ChunkSize)
    : NeXusDataset::ExtensibleDataset<const char>(Parent, "value", CMode,
                                                  ChunkSize) {}
}

void CacheWriter::parse_config(std::string const &ConfigurationStream,
                               std::string const &ConfigurationModule) {}

CacheWriter::InitResult
CacheWriter::init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) {
  const int DefaultChunkSize = ChunkSize.at(0);
  try {
    auto &CurrentGroup = HDFGroup;
    NeXusDataset::Time(             // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueIndex(         // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueTimestampZero( // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
    dataset::Value(                 // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)

    auto ClassAttribute =
        CurrentGroup.attributes.create<std::string>("NX_class");
    ClassAttribute.write("NXnote");
    auto AttributesJson = nlohmann::json::parse(HDFAttributes);
    FileWriter::writeAttributes(HDFGroup, &AttributesJson, Logger);
  } catch (std::exception &E) {
    Logger->error("Unable to initialise NICOS cache data tree in "
                  "HDF file with error message: \"{}\"",
                  E.what());
    return HDFWriterModule::InitResult::ERROR;
  }
  return InitResult::OK;
}

std::int32_t CacheWriter::close() { return 0; }

void CacheWriter::write(FileWriter::FlatbufferMessage const &Message) {
  uint64_t CurrentTimestamp{0};

  Timestamp.appendElement(CurrentTimestamp);
  // if (++CueCounter == CueInterval) {
  //   CueTimestampIndex.appendElement(Timestamp.dataspace().size() - 1);
  //   CueTimestamp.appendElement(CurrentTimestamp);
  //   CueCounter = 0;
  // }
}
} // namespace NicosCacheWriter
