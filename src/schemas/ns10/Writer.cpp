
#include "../../HDFFile.h"
#include "NicosCacheWriter.h"
#include "ns10_cache_entry_generated.h"

namespace NicosCacheWriter {

static FileWriter::HDFWriterModuleRegistry::Registrar<CacheWriter>
    RegisterWriter("ns10");

StringValue::StringValue(hdf5::node::Group const &Parent,
                         NeXusDataset::Mode CMode, size_t ChunkSize)
    : NeXusDataset::ExtensibleDataset<std::string>(Parent, "value", CMode,
                                                   ChunkSize) {}

void CacheWriter::parse_config(std::string const &ConfigurationStream,
                               std::string const &) {
  auto Config = nlohmann::json::parse(ConfigurationStream);
  try {
    CueInterval = Config["cue_interval"].get<uint64_t>();
  } catch (...) {
    // Do nothing
  }

  auto JsonChunkSize = Config["chunk_size"];
  if (JsonChunkSize.is_array()) {
    ChunkSize = Config["chunk_size"].get<hdf5::Dimensions>();
  } else if (JsonChunkSize.is_number_integer()) {
    ChunkSize = hdf5::Dimensions{JsonChunkSize.get<hsize_t>()};
  } else {
    Logger->warn("Unable to extract chunk size, using the default (64). "
                 "This might be very inefficient.");
  }
  try {
    Sourcename = Config["source"];
  } catch (...) {
    Logger->error("Key \"source\" is not specified in json command");
    return;
  }

  Logger->info("Using a cue interval of {}.", CueInterval);
}

using FileWriterBase = FileWriter::HDFWriterModule;

FileWriterBase::InitResult
CacheWriter::init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) {
  const int DefaultChunkSize = ChunkSize.at(0);
  try {
    auto &CurrentGroup = HDFGroup;

    StringValue(                    // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
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
    auto ClassAttribute =
        CurrentGroup.attributes.create<std::string>("NX_class");
    ClassAttribute.write("NXlog");
    auto AttributesJson = nlohmann::json::parse(HDFAttributes);
    FileWriter::writeAttributes(HDFGroup, &AttributesJson, Logger);
  } catch (std::exception &E) {
    Logger->error("Unable to initialise areaDetector data tree in "
                  "HDF file with error message: \"{}\"",
                  E.what());
    return HDFWriterModule::InitResult::ERROR;
  }
  return FileWriterBase::InitResult::OK;
}

FileWriterBase::InitResult CacheWriter::reopen(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
    Values = StringValue(CurrentGroup, NeXusDataset::Mode::Open);
    Timestamp = NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestampIndex =
        NeXusDataset::CueIndex(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestamp =
        NeXusDataset::CueTimestampZero(CurrentGroup, NeXusDataset::Mode::Open);
  } catch (std::exception &E) {
    Logger->error(
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return HDFWriterModule::InitResult::ERROR;
  }
  return FileWriterBase::InitResult::OK;
}

static CacheEntry const *getRoot(char const *Data) {
  return GetCacheEntry(Data);
}

void CacheWriter::write(const FileWriter::FlatbufferMessage &Message) {
  auto Entry = getRoot(Message.data());
  auto CurrentTimestamp = Entry->time();
  auto Source = Entry->key();
  auto Value = Entry->value();
  if (!Source || !Value) {
    throw std::runtime_error("Invalid Flatbuffer content.");
  }

  if (Source->str() != Sourcename) {
    return;
  }

  Values.appendElement(Value->str());

  Timestamp.appendElement(std::lround(1e9 * CurrentTimestamp));
  if (++CueCounter == CueInterval) {
    CueTimestampIndex.appendElement(Timestamp.dataspace().size() - 1);
    CueTimestamp.appendElement(CurrentTimestamp);
    CueCounter = 0;
  }
}

std::int32_t CacheWriter::flush() { return 0; }

std::int32_t CacheWriter::close() { return 0; }

} // namespace NicosCacheWriter
