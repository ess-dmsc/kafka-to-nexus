
#include "../../HDFFile.h"
#include "NicosCacheWriter.h"
#include "ns10_cache_entry_generated.h"

namespace NicosCacheWriter {

// Creates a factory function used to instantiate zero or more CacheWriter,
// i.e.
// one for every data source which produces data with the file id "test".
static FileWriter::HDFWriterModuleRegistry::Registrar<CacheWriter>
    RegisterWriter("ns10");

} // namespace NicosCacheWriter

namespace NicosCacheWriter {

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
} // namespace dataset

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
