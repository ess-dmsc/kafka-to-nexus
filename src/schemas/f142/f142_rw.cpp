#include "f142_rw.h"
#include "../../CollectiveQueue.h"
#include "../../HDFFile.h"
#include "../../helper.h"
#include "../../json.h"
#include "FlatbufferReader.h"
#include "WriterArray.h"
#include "WriterScalar.h"
#include <hdf5.h>
#include <limits>

namespace FileWriter {
namespace Schemas {
namespace f142 {

using nlohmann::json;

/// Used to indicate the rank of the HDF dataset to be written.
enum class Rank {
  SCALAR,
  ARRAY,
};

/// \brief Map Rank and a name of a type to the factory of the corresponding
/// dataset writer.
static std::map<Rank, std::map<std::string, std::unique_ptr<WriterFactory>>>
    RankAndTypenameToValueTraits;

namespace {

/// \brief Define mapping from typename to factory functions for the actual HDF
/// writers.
struct InitTypeMap {
  InitTypeMap() {
    auto &Scalar = RankAndTypenameToValueTraits[Rank::SCALAR];
    auto &Array = RankAndTypenameToValueTraits[Rank::ARRAY];
    // clang-format off
    Scalar[ "uint8"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar< uint8_t, UByte>);
    Scalar["uint16"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<uint16_t, UShort>);
    Scalar["uint32"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<uint32_t, UInt>);
    Scalar["uint64"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<uint64_t, ULong>);
    Scalar[  "int8"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<  int8_t, Byte>);
    Scalar[ "int16"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar< int16_t, Short>);
    Scalar[ "int32"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar< int32_t, Int>);
    Scalar[ "int64"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar< int64_t, Long>);
    Scalar[ "float"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<   float, Float>);
    Scalar["double"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<  double, Double>);
    Scalar["string"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalarString);

    Array[ "uint8"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray< uint8_t, ArrayUByte>);
    Array["uint16"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<uint16_t, ArrayUShort>);
    Array["uint32"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<uint32_t, ArrayUInt>);
    Array["uint64"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<uint64_t, ArrayULong>);
    Array[  "int8"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<  int8_t, ArrayByte>);
    Array[ "int16"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray< int16_t, ArrayShort>);
    Array[ "int32"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray< int32_t, ArrayInt>);
    Array[ "int64"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray< int64_t, ArrayLong>);
    Array[ "float"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<   float, ArrayFloat>);
    Array["double"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<  double, ArrayDouble>);
    // clang-format on
  }
};

/// \brief  Instantiate the typemap.
InitTypeMap TriggerInitTypeMap;
}

///  Helper struct to make branching on a found map entry more concise.
template <typename T> struct FoundInMap {
  FoundInMap() : Value(nullptr) {}
  explicit FoundInMap(T const &Value) : Value(&Value) {}
  bool found() const { return Value != nullptr; }
  T const &value() const { return *Value; }
  T const *Value;
};

///  Helper function to make branching on a found map entry more concise.
template <typename T, typename K>
FoundInMap<typename T::mapped_type> findInMap(T const &Map, K const &Key) {
  auto It = Map.find(Key);
  if (It == Map.end()) {
    return FoundInMap<typename T::mapped_type>();
  }
  return FoundInMap<typename T::mapped_type>(It->second);
}

/// \brief  New DatasetInfo.
///
/// \param  Name                 Name of the dataset
/// \param  ChunkBytes           Chunk size in bytes.
/// \param  BufferSize           Size of in-memory buffer.
/// \param  BufferPacketMaxSize  Maximum size to be bufferd on write.
DatasetInfo::DatasetInfo(std::string Name, size_t ChunkBytes, size_t BufferSize,
                         size_t BufferPacketMaxSize,
                         uptr<h5::h5d_chunked_1d<uint64_t>> &Ptr)
    : Name(std::move(Name)), ChunkBytes(ChunkBytes), BufferSize(BufferSize),
      BufferPacketMaxSize(BufferPacketMaxSize), Ptr(Ptr) {}

/// \brief Instantiate a new writer.
///
/// \param HDFGroup The HDF group into which this writer will place the dataset.
/// \param ArraySize Zero if scalar, or the size of the array.
/// \param TypeName The name of the datatype to be written.
/// \param Datasetname Name of the dataset to be written.
/// \param cq (Currently used on experimental branch, remove from this PR)
/// \param HDFStore (Currently used on experimental branch, remove from this PR)
/// \param Method Either CREATE or OPEN, will either create the HDF dataset, or
/// open the dataset again.
std::unique_ptr<WriterTypedBase>
createWriterTypedBase(hdf5::node::Group HDFGroup, size_t ArraySize,
                      std::string TypeName, std::string DatasetName,
                      CollectiveQueue *cq, HDFIDStore *HDFStore,
                      CreateWriterTypedBaseMethod Method) {
  Rank TheRank = Rank::SCALAR;
  if (ArraySize > 0) {
    TheRank = Rank::ARRAY;
  }
  auto ValueTraitsMaybe =
      findInMap<std::map<std::string, std::unique_ptr<WriterFactory>>>(
          RankAndTypenameToValueTraits[TheRank], TypeName);
  if (!ValueTraitsMaybe.found()) {
    LOG(Sev::Error, "Could not get ValueTraits for TypeName: {}  ArraySize: {} "
                    " RankAndTypenameToValueTraits.size(): {}",
        TypeName, ArraySize, RankAndTypenameToValueTraits[TheRank].size());
    return nullptr;
  }
  auto const &ValueTraits = ValueTraitsMaybe.value();
  if (Method == CreateWriterTypedBaseMethod::OPEN) {
    return ValueTraits->createWriter(HDFGroup, DatasetName, ArraySize,
                                     ValueTraits->getValueUnionID(), cq,
                                     HDFStore);
  }
  return ValueTraits->createWriter(HDFGroup, DatasetName, ArraySize,
                                   ValueTraits->getValueUnionID(), cq);
}

/// Parse the configuration for this stream.
void HDFWriterModule::parse_config(std::string const &ConfigurationStream,
                                   std::string const &ConfigurationModule) {
  auto ConfigurationStreamJson = json::parse(ConfigurationStream);
  if (auto SourceNameMaybe =
          find<std::string>("source", ConfigurationStreamJson)) {
    SourceName = SourceNameMaybe.inner();
  } else {
    LOG(Sev::Error, "Key \"source\" is not specified in json command");
    return;
  }

  if (auto TypeNameMaybe = find<std::string>("type", ConfigurationStreamJson)) {
    TypeName = TypeNameMaybe.inner();
  } else {
    throw std::runtime_error(
        fmt::format("Missing key \"type\" in f142 writer configuration"));
  }

  if (auto ArraySizeMaybe =
          find<uint64_t>("array_size", ConfigurationStreamJson)) {
    ArraySize = size_t(ArraySizeMaybe.inner());
  }

  LOG(Sev::Debug,
      "HDFWriterModule::parse_config f142 SourceName: {}  type: {}  "
      "array_size: {}",
      SourceName, TypeName, ArraySize);

  try {
    IndexEveryBytes =
        ConfigurationStreamJson["nexus"]["indices"]["index_every_kb"]
            .get<uint64_t>() *
        1024;
    LOG(Sev::Debug, "index_every_bytes: {}", IndexEveryBytes);
  } catch (...) { /* it's ok if not found */
  }
  try {
    IndexEveryBytes =
        ConfigurationStreamJson["nexus"]["indices"]["index_every_mb"]
            .get<uint64_t>() *
        1024 * 1024;
    LOG(Sev::Debug, "index_every_bytes: {}", IndexEveryBytes);
  } catch (...) { /* it's ok if not found */
  }
  if (ConfigurationStreamJson.find("store_latest_into") !=
      ConfigurationStreamJson.end()) {
    StoreLatestInto = ConfigurationStreamJson["store_latest_into"];
    LOG(Sev::Debug, "StoreLatestInto: {}", StoreLatestInto);
  }
}

/// Initialize some parameters and the list of datasets to be created.
HDFWriterModule::HDFWriterModule() {
  // Setup the parameters for our datasets
  size_t ChunkBytes = 64 * 1024;
  size_t BufferSize = 16 * 1024;
  size_t BufferPacketMaxSize = 1024;
  DatasetInfoList.emplace_back("time", ChunkBytes, BufferSize,
                               BufferPacketMaxSize, DatasetTimestamp);
  DatasetInfoList.emplace_back("cue_timestamp_zero", ChunkBytes, BufferSize,
                               BufferPacketMaxSize, DatasetCueTimestampZero);
  DatasetInfoList.emplace_back("cue_index", ChunkBytes, BufferSize,
                               BufferPacketMaxSize, DatasetCueIndex);
  if (DoWriteForwarderInternalDebugInfo) {
    DatasetInfoList.emplace_back("__fwdinfo_seq_data", ChunkBytes, BufferSize,
                                 BufferPacketMaxSize, DatasetSeqData);
    DatasetInfoList.emplace_back("__fwdinfo_seq_fwd", ChunkBytes, BufferSize,
                                 BufferPacketMaxSize, DatasetSeqFwd);
    DatasetInfoList.emplace_back("__fwdinfo_ts_data", ChunkBytes, BufferSize,
                                 BufferPacketMaxSize, DatasetTsData);
  }
}

/// \brief Implement the HDFWriterModule interface, forward to the CREATE case
/// of
/// `init_hdf`.
HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &HDFGroup,
                          std::string const &HDFAttributes) {
  return init_hdf(HDFGroup, &HDFAttributes,
                  CreateWriterTypedBaseMethod::CREATE);
}

/// \brief Implement the HDFWriterModule interface, forward to the OPEN case of
/// `init_hdf`.
HDFWriterModule::InitResult
HDFWriterModule::reopen(hdf5::node::Group &HDFGroup) {
  return init_hdf(HDFGroup, nullptr, CreateWriterTypedBaseMethod::OPEN);
}

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &HDFGroup,
                          std::string const *HDFAttributes,
                          CreateWriterTypedBaseMethod CreateMethod) {
  try {
    HDFIDStore *HDFStore =
        nullptr; // These two lines should probably be deleted.
    CollectiveQueue *cq = nullptr;
    ValueWriter = createWriterTypedBase(HDFGroup, ArraySize, TypeName, "value",
                                        cq, HDFStore, CreateMethod);
    if (!ValueWriter) {
      LOG(Sev::Error,
          "Could not create a writer implementation for value_type {}",
          TypeName);
      return HDFWriterModule::InitResult::ERROR_IO();
    }
    if (CreateMethod == CreateWriterTypedBaseMethod::CREATE) {
      for (auto const &Info : DatasetInfoList) {
        Info.Ptr = h5::h5d_chunked_1d<uint64_t>::create(HDFGroup, Info.Name,
                                                        Info.ChunkBytes, cq);
        if (Info.Ptr.get() == nullptr) {
          return HDFWriterModule::InitResult::ERROR_IO();
        }
      }
      auto AttributesJson = nlohmann::json::parse(*HDFAttributes);
      HDFFile::writeAttributes(HDFGroup, &AttributesJson);
    } else if (CreateMethod == CreateWriterTypedBaseMethod::OPEN) {
      for (auto const &Info : DatasetInfoList) {
        Info.Ptr = h5::h5d_chunked_1d<uint64_t>::open(HDFGroup, Info.Name, cq,
                                                      HDFStore);
        if (Info.Ptr.get() == nullptr) {
          return HDFWriterModule::InitResult::ERROR_IO();
        }
        Info.Ptr->buffer_init(Info.BufferSize, Info.BufferPacketMaxSize);
      }
    }
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    std::throw_with_nested(std::runtime_error(fmt::format(
        "f142 could not init HDFGroup: {}  trace: {}",
        static_cast<std::string>(HDFGroup.link().path()), message)));
  }
  return HDFWriterModule::InitResult::OK();
}

/// \brief  Inspect the incoming FlatBuffer from the message and write the
/// content to datasets.
HDFWriterModule::WriteResult
HDFWriterModule::write(FlatbufferMessage const &Message) {
  auto fbuf = get_fbuf(Message.data());
  if (!ValueWriter) {
    auto Now = CLOCK::now();
    if (Now > TimestampLastErrorLog + ErrorLogMinInterval) {
      TimestampLastErrorLog = Now;
      LOG(Sev::Warning,
          "sorry, but we were unable to initialize for this kind of messages");
    }
    return HDFWriterModule::WriteResult::ERROR_IO();
  }
  auto wret = ValueWriter->write(fbuf);
  if (!wret) {
    auto Now = CLOCK::now();
    if (Now > TimestampLastErrorLog + ErrorLogMinInterval) {
      TimestampLastErrorLog = Now;
      LOG(Sev::Error, "write failed: {}", wret.ErrorString);
    }
  }
  WrittenBytesTotal += wret.written_bytes;
  TimestampMax = std::max(fbuf->timestamp(), TimestampMax);
  if (WrittenBytesTotal > IndexAtBytes + IndexEveryBytes) {
    this->DatasetCueTimestampZero->append_data_1d(&TimestampMax, 1);
    this->DatasetCueIndex->append_data_1d(&wret.ix0, 1);
    IndexAtBytes = WrittenBytesTotal;
  }
  {
    auto x = fbuf->timestamp();
    this->DatasetTimestamp->append_data_1d(&x, 1);
  }
  if (DoWriteForwarderInternalDebugInfo) {
    if (fbuf->fwdinfo_type() == forwarder_internal::fwdinfo_1_t) {
      auto fi = (fwdinfo_1_t *)fbuf->fwdinfo();
      {
        auto x = fi->seq_data();
        this->DatasetSeqData->append_data_1d(&x, 1);
      }
      {
        auto x = fi->seq_fwd();
        this->DatasetSeqFwd->append_data_1d(&x, 1);
      }
      {
        auto x = fi->ts_data();
        this->DatasetTsData->append_data_1d(&x, 1);
      }
    }
  }
  return HDFWriterModule::WriteResult::OK_WITH_TIMESTAMP(fbuf->timestamp());
}

/// Implement HDFWriterModule interface, just flushing.
int32_t HDFWriterModule::flush() {
  for (auto const &Info : DatasetInfoList) {
    Info.Ptr->flush_buf();
  }
  return 0;
}

/// Implement HDFWriterModule interface.
int32_t HDFWriterModule::close() {
  if (!StoreLatestInto.empty()) {
    ValueWriter->storeLatestInto(StoreLatestInto);
  }
  for (auto const &Info : DatasetInfoList) {
    Info.Ptr.reset();
  }
  return 0;
}

/// Register the writer module.
static HDFWriterModuleRegistry::Registrar<HDFWriterModule>
    RegisterWriter("f142");

} // namespace f142
} // namespace Schemas
} // namespace FileWriter
