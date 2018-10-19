/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
/// \brief Implement classes required to implement the ADC file writing module.

#include "../../helper.h"

#include "AreaDetectorWriter.h"
#include "HDFFile.h"

namespace NDAr {

// Register the timestamp and name extraction class for this module.
static FileWriter::FlatbufferReaderRegistry::Registrar<AreaDetectorDataGuard>
    RegisterNDArGuard("NDAr");

// Register the file writing part of this module.
static FileWriter::HDFWriterModuleRegistry::Registrar<AreaDetectorWriter>
    RegisterNDArWriter("NDAr");

bool AreaDetectorDataGuard::verify(FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const std::uint8_t *const>(Message.data()),
      Message.size());
  return FB_Tables::VerifyNDArrayBuffer(Verifier);
}

std::uint64_t epicsTimeToNsec(std::uint64_t sec, std::uint64_t nsec) {
  const auto TimeDiffUNIXtoEPICSepoch = 631152000L;
  const auto NSecMultiplier = 1000000000L;
  return (sec + TimeDiffUNIXtoEPICSepoch) * NSecMultiplier + nsec;
}

uint64_t
AreaDetectorDataGuard::timestamp(FlatbufferMessage const &Message) const {
  auto FbPointer = FB_Tables::GetNDArray(Message.data());
  auto epicsTime = FbPointer->epicsTS();
  return epicsTimeToNsec(epicsTime->secPastEpoch(), epicsTime->nsec());
}

std::string AreaDetectorDataGuard::source_name(
    const FileWriter::FlatbufferMessage &Message) const {
  // The source name was left out of the relevant EPICS areaDetector plugin.
  // There is currently a pull request for adding this variable to the FB
  // schema. When the variable has been addded, this function will be updated.
  return "ADPluginKafka";
}

/// \brief Parse config JSON structure.
///
/// The default is to use double as the element type.
void AreaDetectorWriter::parse_config(std::string const &ConfigurationStream,
                                      std::string const &ConfigurationModule) {
  auto Config = nlohmann::json::parse(ConfigurationStream);
  try {
    CueInterval = Config["cue_interval"].get<uint64_t>();
  } catch (...) {
    // Do nothing
  }
  try {
    auto DataType = Config["type"].get<std::string>();
    std::map<std::string, AreaDetectorWriter::Type> TypeMap{
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
      LOG(Sev::Error, "Unknown type ({}), using the default (double).",
          DataType);
    }
  } catch (nlohmann::json::exception &E) {
    LOG(Sev::Warning, "Unable to extract data type, using the default "
                      "(double). Error was: {}",
        E.what());
  }

  try {
    ArrayShape = Config["array_size"].get<hdf5::Dimensions>();
  } catch (nlohmann::json::exception &E) {
    LOG(Sev::Warning,
        "Unable to extract array size, using the default (1x1). Error was: {}",
        E.what());
  }

  auto JsonChunkSize = Config["chunk_size"];
  if (JsonChunkSize.is_array()) {
    ChunkSize = Config["chunk_size"].get<hdf5::Dimensions>();
  } else if (JsonChunkSize.is_number_integer()) {
    ChunkSize = hdf5::Dimensions{JsonChunkSize.get<hsize_t>()};
  } else {
    LOG(Sev::Warning, "Unable to extract chunk size, using the default (64). "
                      "This might be very inefficient.");
  }
  LOG(Sev::Info, "Using a cue interval of {}.", CueInterval);
}

FileWriterBase::InitResult
AreaDetectorWriter::init_hdf(hdf5::node::Group &HDFGroup,
                             std::string const &HDFAttributes) {
  const int DefaultChunkSize = ChunkSize.at(0);
  try {
    auto &CurrentGroup = HDFGroup;
    initValueDataset(CurrentGroup);
    NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Create,
                       DefaultChunkSize);
    NeXusDataset::CueIndex(CurrentGroup, NeXusDataset::Mode::Create,
                           DefaultChunkSize);
    NeXusDataset::CueTimestampZero(CurrentGroup, NeXusDataset::Mode::Create,
                                   DefaultChunkSize);
    auto ClassAttribute =
        CurrentGroup.attributes.create<std::string>("NX_class");
    ClassAttribute.write("NXlog");
    auto AttributesJson = nlohmann::json::parse(HDFAttributes);
    FileWriter::HDFFile::write_attributes(HDFGroup, &AttributesJson);
  } catch (std::exception &E) {
    LOG(Sev::Error, "Unable to initialise areaDetector data tree in "
                    "HDF file with error message: \"{}\"",
        E.what());
    return HDFWriterModule::InitResult::ERROR_IO();
  }
  return FileWriterBase::InitResult::OK();
}

FileWriterBase::InitResult
AreaDetectorWriter::reopen(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
    Values = std::make_unique<NeXusDataset::MultiDimDatasetBase>(
        CurrentGroup, NeXusDataset::Mode::Open);
    Timestamp = NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestampIndex =
        NeXusDataset::CueIndex(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestamp =
        NeXusDataset::CueTimestampZero(CurrentGroup, NeXusDataset::Mode::Open);
  } catch (std::exception &E) {
    LOG(Sev::Error,
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return HDFWriterModule::InitResult::ERROR_IO();
  }
  return FileWriterBase::InitResult::OK();
}
template <typename DataType, class DatasetType>
void appendData(DatasetType &Dataset, const std::uint8_t *Pointer, size_t Size,
                hdf5::Dimensions const &Shape) {
  Dataset->appendArray(
      ArrayAdapter<DataType>(reinterpret_cast<DataType *>(Pointer), Size),
      Shape);
}

FileWriterBase::WriteResult
AreaDetectorWriter::write(const FileWriter::FlatbufferMessage &Message) {
  auto NDAr = FB_Tables::GetNDArray(Message.data());
  auto DataShape = hdf5::Dimensions(NDAr->dims()->begin(), NDAr->dims()->end());
  auto CurrentTimestamp =
      epicsTimeToNsec(NDAr->epicsTS()->secPastEpoch(), NDAr->epicsTS()->nsec());
  FB_Tables::DType Type = NDAr->dataType();
  auto DataPtr = NDAr->pData()->Data();
  auto NrOfElements = std::accumulate(
      std::begin(DataShape), std::end(DataShape), 1, std::multiplies<double>());

  switch (Type) {
  case FB_Tables::DType::Int8:
    appendData<const std::int8_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Uint8:
    appendData<const std::uint8_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Int16:
    appendData<const std::int16_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Uint16:
    appendData<const std::uint16_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Int32:
    appendData<const std::int32_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Uint32:
    appendData<const std::uint32_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Float32:
    appendData<const float>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Float64:
    appendData<const double>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::c_string:
    appendData<const char>(Values, DataPtr, NrOfElements, DataShape);
    break;
  default:
    return FileWriterBase::WriteResult::ERROR_BAD_FLATBUFFER();
    break;
  }
  Timestamp.appendElement(CurrentTimestamp);
  if (++CueCounter == CueInterval) {
    CueTimestampIndex.appendElement(Timestamp.dataspace().size() - 1);
    CueTimestamp.appendElement(CurrentTimestamp);
    CueCounter = 0;
  }
  return FileWriterBase::WriteResult::OK();
}

std::int32_t AreaDetectorWriter::flush() { return 0; }

std::int32_t AreaDetectorWriter::close() { return 0; }

void AreaDetectorWriter::enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                                   int mpi_rank) {
  LOG(Sev::Error, "Collective queue not implemented.");
}
template <typename Type>
std::unique_ptr<NeXusDataset::MultiDimDatasetBase>
makeIt(hdf5::node::Group &Parent, hdf5::Dimensions &Shape,
       hdf5::Dimensions const &ChunkSize) {
  return std::make_unique<NeXusDataset::MultiDimDataset<Type>>(
      Parent, NeXusDataset::Mode::Create, Shape, ChunkSize);
}

void AreaDetectorWriter::initValueDataset(hdf5::node::Group &Parent) {
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
} // namespace NDAr
