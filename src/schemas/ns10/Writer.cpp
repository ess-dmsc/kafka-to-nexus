
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

StringValue::StringValue(hdf5::node::Group const &Parent,
                         NeXusDataset::Mode CMode, size_t ChunkSize)
    : NeXusDataset::ExtensibleDataset<std::string>(Parent, "value", CMode,
                                                   ChunkSize) {}

// namespace dataset {
// class Value : public NeXusDataset::ExtensibleDataset<const char> {
// public:
//   Value() = default;
//   /// \brief Create the time dataset of NXLog.
//   /// \throw std::runtime_error if dataset already exists.
//   Value(hdf5::node::Group const &Parent, NeXusDataset::Mode CMode,
//         size_t ChunkSize = 1024);
// };
// Value::Value(hdf5::node::Group const &Parent, NeXusDataset::Mode CMode,
//              size_t ChunkSize)
//     : NeXusDataset::ExtensibleDataset<const char>(Parent, "value", CMode,
//                                                   ChunkSize) {}
// } // namespace dataset
//
// void CacheWriter::parse_config(std::string const &ConfigurationStream,
//                                std::string const &ConfigurationModule) {
//   auto Config = nlohmann::json::parse(ConfigurationStream);
//   try {
//     auto DataType = Config["type"].get<std::string>();
//     std::map<std::string, CacheWriter::Type> TypeMap{
//         {"int8", Type::int8},         {"uint8", Type::uint8},
//         {"int16", Type::int16},       {"uint16", Type::uint16},
//         {"int32", Type::int32},       {"uint32", Type::uint32},
//         {"int64", Type::int64},       {"uint64", Type::uint64},
//         {"float32", Type::float32},   {"float64", Type::float64},
//         {"c_string", Type::c_string},
//     };
//     try {
//       ElementType = TypeMap.at(DataType);
//     } catch (std::out_of_range &E) {
//       Logger->error("Unknown type ({}), using the default (double).",
//       DataType);
//     }
//   } catch (nlohmann::json::exception &E) {
//     Logger->warn("Unable to extract data type, using the default "
//                  "(double). Error was: {}",
//                  E.what());
//   }
//
//   try {
//     ArrayShape = Config["array_size"].get<hdf5::Dimensions>();
//   } catch (nlohmann::json::exception &E) {
//     Logger->warn(
//         "Unable to extract array size, using the default (1x1). Error was:
//         {}",
//         E.what());
//   }
// }
//
// CacheWriter::InitResult
// CacheWriter::init_hdf(hdf5::node::Group &HDFGroup,
//                       std::string const &HDFAttributes) {
//   const int DefaultChunkSize = ChunkSize.at(0);
//   try {
//     auto &CurrentGroup = HDFGroup;
//     initValueDataset(CurrentGroup);
//
//     NeXusDataset::Time(             // NOLINT(bugprone-unused-raii)
//         CurrentGroup,               // NOLINT(bugprone-unused-raii)
//         NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
//         DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
//     NeXusDataset::CueIndex(         // NOLINT(bugprone-unused-raii)
//         CurrentGroup,               // NOLINT(bugprone-unused-raii)
//         NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
//         DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
//     NeXusDataset::CueTimestampZero( // NOLINT(bugprone-unused-raii)
//         CurrentGroup,               // NOLINT(bugprone-unused-raii)
//         NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
//         DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
//     dataset::Value(                 // NOLINT(bugprone-unused-raii)
//         CurrentGroup,               // NOLINT(bugprone-unused-raii)
//         NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
//         DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
//
//     auto ClassAttribute =
//         CurrentGroup.attributes.create<std::string>("NX_class");
//     ClassAttribute.write("NXlog");
//     auto AttributesJson = nlohmann::json::parse(HDFAttributes);
//     FileWriter::writeAttributes(HDFGroup, &AttributesJson, Logger);
//   } catch (std::exception &E) {
//     Logger->error("Unable to initialise NICOS cache data tree in "
//                   "HDF file with error message: \"{}\"",
//                   E.what());
//     return HDFWriterModule::InitResult::ERROR;
//   }
//   return InitResult::OK;
// }
//
// std::int32_t CacheWriter::close() { return 0; }
//
// void CacheWriter::write(FileWriter::FlatbufferMessage const &Message) {
//   uint64_t CurrentTimestamp{0};
//
//   Timestamp.appendElement(CurrentTimestamp);
//   // if (++CueCounter == CueInterval) {
//   //   CueTimestampIndex.appendElement(Timestamp.dataspace().size() - 1);
//   //   CueTimestamp.appendElement(CurrentTimestamp);
//   //   CueCounter = 0;
//   // }
// }
//
// template <typename Type>
// std::unique_ptr<NeXusDataset::MultiDimDatasetBase>
// makeIt(hdf5::node::Group const &Parent, hdf5::Dimensions const &Shape,
//        hdf5::Dimensions const &ChunkSize) {
//   return std::make_unique<NeXusDataset::MultiDimDataset<Type>>(
//       Parent, NeXusDataset::Mode::Create, Shape, ChunkSize);
// }
//
// void CacheWriter::initValueDataset(hdf5::node::Group &Parent) {
//   using OpenFuncType =
//       std::function<std::unique_ptr<NeXusDataset::MultiDimDatasetBase>()>;
//   std::map<Type, OpenFuncType> CreateValuesMap{
//       {Type::c_string,
//        [&Parent, this]() {
//          return makeIt<char>(Parent, this->ArrayShape, this->ChunkSize);
//        }},
//       {Type::int8,
//        [&Parent, this]() {
//          return makeIt<std::int8_t>(Parent, this->ArrayShape,
//          this->ChunkSize);
//        }},
//       {Type::uint8,
//        [&Parent, this]() {
//          return makeIt<std::uint8_t>(Parent, this->ArrayShape,
//          this->ChunkSize);
//        }},
//       {Type::int16,
//        [&Parent, this]() {
//          return makeIt<std::int16_t>(Parent, this->ArrayShape,
//          this->ChunkSize);
//        }},
//       {Type::uint16,
//        [&Parent, this]() {
//          return makeIt<std::uint16_t>(Parent, this->ArrayShape,
//                                       this->ChunkSize);
//        }},
//       {Type::int32,
//        [&Parent, this]() {
//          return makeIt<std::int32_t>(Parent, this->ArrayShape,
//          this->ChunkSize);
//        }},
//       {Type::uint32,
//        [&Parent, this]() {
//          return makeIt<std::uint32_t>(Parent, this->ArrayShape,
//                                       this->ChunkSize);
//        }},
//       {Type::int64,
//        [&Parent, this]() {
//          return makeIt<std::int64_t>(Parent, this->ArrayShape,
//          this->ChunkSize);
//        }},
//       {Type::uint64,
//        [&Parent, this]() {
//          return makeIt<std::uint64_t>(Parent, this->ArrayShape,
//                                       this->ChunkSize);
//        }},
//       {Type::float32,
//        [&Parent, this]() {
//          return makeIt<std::float_t>(Parent, this->ArrayShape,
//          this->ChunkSize);
//        }},
//       {Type::float64,
//        [&Parent, this]() {
//          return makeIt<std::double_t>(Parent, this->ArrayShape,
//                                       this->ChunkSize);
//        }},
//   };
//   Values = CreateValuesMap.at(ElementType)();
// }

void CacheWriter::parse_config(std::string const &ConfigurationStream,
                               std::string const &) {
  auto Config = nlohmann::json::parse(ConfigurationStream);
  try {
    CueInterval = Config["cue_interval"].get<uint64_t>();
  } catch (...) {
    // Do nothing
  }
  try {
    auto DataType = Config["type"].get<std::string>();
    std::map<std::string, CacheWriter::Type> TypeMap{
        {"int8", Type::int8},         {"uint8", Type::uint8},
        {"int16", Type::int16},       {"uint16", Type::uint16},
        {"int32", Type::int32},       {"uint32", Type::uint32},
        {"int64", Type::int64},       {"uint64", Type::uint64},
        {"float32", Type::float32},   {"float64", Type::float64},
        {"c_string", Type::c_string},
    };
    try {
      ElementType = TypeMap.at(DataType);
    } catch (std::out_of_range &E) {
      Logger->error("Unknown type ({}), using the default (double).", DataType);
    }
  } catch (nlohmann::json::exception &E) {
    Logger->warn("Unable to extract data type, using the default "
                 "(double). Error was: {}",
                 E.what());
  }

  try {
    ArrayShape = Config["array_size"].get<hdf5::Dimensions>();
  } catch (nlohmann::json::exception &E) {
    Logger->warn("Unable to extract array size, using the default (1x1). "
                 "Error was: {}",
                 E.what());
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
  Logger->info("Using a cue interval of {}.", CueInterval);
}

using FileWriterBase = FileWriter::HDFWriterModule;

FileWriterBase::InitResult
CacheWriter::init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) {
  const int DefaultChunkSize = ChunkSize.at(0);
  try {
    auto &CurrentGroup = HDFGroup;
    // initValueDataset(CurrentGroup);

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
    // Values = std::make_unique<NeXusDataset::MultiDimDatasetBase>(
    //     CurrentGroup, NeXusDataset::Mode::Open);
    sValues = StringValue(CurrentGroup, NeXusDataset::Mode::Open);
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

template <typename DataType, class DatasetType>
void appendData(DatasetType &Dataset, const std::uint8_t *Pointer, size_t Size,
                hdf5::Dimensions const &Shape) {
  Dataset->appendArray(
      ArrayAdapter<DataType>(reinterpret_cast<DataType *>(Pointer), Size),
      Shape);
}

static CacheEntry const *getRoot(char const *Data) {
  return GetCacheEntry(Data);
}

void CacheWriter::write(const FileWriter::FlatbufferMessage &Message) {
  // auto CurrentTimestamp = Message.getTimestamp();
  auto Entry = getRoot(Message.data());
  auto Source = Entry->key();
  auto CurrentTimestamp = Entry->time();
  auto Value = Entry->value();
  if (Entry) {
    std::cout << Source->str() << "\t" << CurrentTimestamp << "\t"
              << Value->str();
    std::cout << "\n";
  } else {
    std::cout << "invalid entry\n";
  }

  // uint64_t CurrentTimestamp{0};
  // auto Verifier = flatbuffers::Verifier(
  //     reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  // std::cout << "Verifier: " << VerifyCacheEntryBuffer(Verifier) << "\n";
  // // epicsTimeToNsec(NDAr->epicsTS()->secPastEpoch(),
  // NDAr->epicsTS()->nsec());
  // FB_Tables::DType Type = NDAr->dataType();
  // auto DataPtr = NDAr->pData()->Data();
  // auto NrOfElements = std::accumulate(
  //     std::begin(DataShape), std::end(DataShape), 1, std::multiplies<>());
  //
  // switch (Type) {
  // case FB_Tables::DType::Int8:
  //   appendData<const std::int8_t>(Values, DataPtr, NrOfElements,
  //   DataShape);
  //   break;
  // case FB_Tables::DType::Uint8:
  //   appendData<const std::uint8_t>(Values, DataPtr, NrOfElements,
  //   DataShape);
  //   break;
  // case FB_Tables::DType::Int16:
  //   appendData<const std::int16_t>(Values, DataPtr, NrOfElements,
  //   DataShape);
  //   break;
  // case FB_Tables::DType::Uint16:
  //   appendData<const std::uint16_t>(Values, DataPtr, NrOfElements,
  //   DataShape);
  //   break;
  // case FB_Tables::DType::Int32:
  //   appendData<const std::int32_t>(Values, DataPtr, NrOfElements,
  //   DataShape);
  //   break;
  // case FB_Tables::DType::Uint32:
  //   appendData<const std::uint32_t>(Values, DataPtr, NrOfElements,
  //   DataShape);
  //   break;
  // case FB_Tables::DType::Float32:
  //   appendData<const float>(Values, DataPtr, NrOfElements, DataShape);
  //   break;
  // case FB_Tables::DType::Float64:
  //   appendData<const double>(Values, DataPtr, NrOfElements, DataShape);
  //   break;
  // case FB_Tables::DType::c_string:
  //   appendData<const char>(Values, DataPtr, NrOfElements, DataShape);
  //   break;
  // default:
  //   throw FileWriter::HDFWriterModuleRegistry::WriterException(
  //       "Error in flatbuffer.");
  // }
  sValues.appendElement(Value->str());
  Timestamp.appendElement(CurrentTimestamp);
  if (++CueCounter == CueInterval) {
    CueTimestampIndex.appendElement(Timestamp.dataspace().size() - 1);
    CueTimestamp.appendElement(CurrentTimestamp);
    CueCounter = 0;
  }
}

// std::int32_t CacheWriter::flush() { return 0; }

std::int32_t CacheWriter::close() { return 0; }

template <typename Type>
std::unique_ptr<NeXusDataset::MultiDimDatasetBase>
makeIt(hdf5::node::Group const &Parent, hdf5::Dimensions const &Shape,
       hdf5::Dimensions const &ChunkSize) {
  return std::make_unique<NeXusDataset::MultiDimDataset<Type>>(
      Parent, NeXusDataset::Mode::Create, Shape, ChunkSize);
}

void CacheWriter::initValueDataset(hdf5::node::Group &Parent) {
  using OpenFuncType =
      std::function<std::unique_ptr<NeXusDataset::MultiDimDatasetBase>()>;
  std::map<Type, OpenFuncType> CreateValuesMap{
      {Type::c_string,
       [&Parent, this]() {
         return makeIt<char>(Parent, this->ArrayShape, this->ChunkSize);
       }},
      {Type::int8,
       [&Parent, this]() {
         return makeIt<std::int8_t>(Parent, this->ArrayShape, this->ChunkSize);
       }},
      {Type::uint8,
       [&Parent, this]() {
         return makeIt<std::uint8_t>(Parent, this->ArrayShape, this->ChunkSize);
       }},
      {Type::int16,
       [&Parent, this]() {
         return makeIt<std::int16_t>(Parent, this->ArrayShape, this->ChunkSize);
       }},
      {Type::uint16,
       [&Parent, this]() {
         return makeIt<std::uint16_t>(Parent, this->ArrayShape,
                                      this->ChunkSize);
       }},
      {Type::int32,
       [&Parent, this]() {
         return makeIt<std::int32_t>(Parent, this->ArrayShape, this->ChunkSize);
       }},
      {Type::uint32,
       [&Parent, this]() {
         return makeIt<std::uint32_t>(Parent, this->ArrayShape,
                                      this->ChunkSize);
       }},
      {Type::int64,
       [&Parent, this]() {
         return makeIt<std::int64_t>(Parent, this->ArrayShape, this->ChunkSize);
       }},
      {Type::uint64,
       [&Parent, this]() {
         return makeIt<std::uint64_t>(Parent, this->ArrayShape,
                                      this->ChunkSize);
       }},
      {Type::float32,
       [&Parent, this]() {
         return makeIt<std::float_t>(Parent, this->ArrayShape, this->ChunkSize);
       }},
      {Type::float64,
       [&Parent, this]() {
         return makeIt<std::double_t>(Parent, this->ArrayShape,
                                      this->ChunkSize);
       }},
  };
  Values = CreateValuesMap.at(ElementType)();
}

} // namespace NicosCacheWriter
