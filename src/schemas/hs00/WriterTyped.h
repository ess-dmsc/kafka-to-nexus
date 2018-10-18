#pragma once

#include "Exceptions.h"
#include "HistogramRecord.h"
#include "Shape.h"
#include "WriterUntyped.h"
#include "helper.h"
#include "json.h"
#include <flatbuffers/flatbuffers.h>
#include <h5cpp/hdf5.hpp>
#include <type_traits>
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

#include "schemas/hs00_event_histogram_generated.h"

template <typename DataType, typename EdgeType, typename ErrorType>
class WriterTyped : public WriterUntyped {
private:
  using json = nlohmann::json;

public:
  using ptr = std::unique_ptr<WriterTyped<DataType, EdgeType, ErrorType>>;

  /// Create a WriterTyped from Json, used when a new write command arrives at
  /// the file writer.
  static ptr createFromJson(json const &Json);

  static ptr createFromHDF(hdf5::node::Group &Group);

  /// Create the HDF structures. used on arrival of a new write command for the
  /// file writer to create the initial structure of the HDF file for this
  /// writer module.
  void createHDFStructure(hdf5::node::Group &Group, size_t ChunkBytes) override;

  HDFWriterModule::WriteResult write(FlatbufferMessage const &Message,
                                     bool DoFlushEachWrite) override;

  ~WriterTyped() override;
  int close() override;
  int copyLatestToData(size_t HDFIndex);

private:
  Shape<EdgeType> TheShape;
  std::string CreatedFromJson;
  hdf5::node::Dataset Dataset;
  hdf5::node::Dataset DatasetErrors;
  hdf5::node::Dataset DatasetTimestamps;
  hdf5::node::Dataset DatasetInfo;
  hdf5::node::Dataset DatasetInfoTimestamp;

  // clang-format off
  using FlatbufferDataType =
  typename std::conditional<std::is_same<DataType, uint32_t>::value, ArrayUInt,
  typename std::conditional<std::is_same<DataType, uint64_t>::value, ArrayULong,
  typename std::conditional<std::is_same<DataType,   double>::value, ArrayDouble,
  typename std::conditional<std::is_same<DataType,    float>::value, ArrayFloat,
  std::nullptr_t>::type>::type>::type>::type;

  using FlatbufferErrorType =
  typename std::conditional<std::is_same<ErrorType, uint32_t>::value, ArrayUInt,
  typename std::conditional<std::is_same<ErrorType, uint64_t>::value, ArrayULong,
  typename std::conditional<std::is_same<ErrorType,   double>::value, ArrayDouble,
  typename std::conditional<std::is_same<ErrorType,    float>::value, ArrayFloat,
  std::nullptr_t>::type>::type>::type>::type;
  // clang-format on

  std::map<uint64_t, HistogramRecord> HistogramRecords;
  std::vector<HistogramRecord> HistogramRecordsFreed;

  size_t ChunkBytes = 1 * 1024 * 1024;
  bool DoConvertEdgeTypeToFloat = false;

  uint64_t LargestTimestampSeen = 0;
  size_t MaxNumberHistoric = 4;
};

template <typename DataType, typename EdgeType, typename ErrorType>
WriterTyped<DataType, EdgeType, ErrorType>::~WriterTyped() {
  LOG(Sev::Debug, "WriterTyped dtor");
}

template <typename DataType, typename EdgeType, typename ErrorType>
int WriterTyped<DataType, EdgeType, ErrorType>::close() {
  // Currently copy after each full write
  // return copyLatestToData();
  return 0;
}

template <typename DataType, typename EdgeType, typename ErrorType>
int WriterTyped<DataType, EdgeType, ErrorType>::copyLatestToData(
    size_t HDFIndex) {
  LOG(Sev::Debug, "WriterTyped copyLatestToData");
  if (Dataset.is_valid()) {
    LOG(Sev::Debug, "Found valid dataset");
    auto Type = hdf5::datatype::create<DataType>().native_type();
    auto SpaceIn = hdf5::dataspace::Simple(Dataset.dataspace());
    auto Dims = SpaceIn.current_dimensions();
    auto Offset = Dims;
    auto Block = Dims;
    Offset.at(0) = HDFIndex;
    for (size_t I = 1; I < Offset.size(); ++I) {
      Offset.at(I) = 0;
    }
    Block.at(0) = 1;
    SpaceIn.selection(hdf5::dataspace::SelectionOperation::SET,
                      hdf5::dataspace::Hyperslab(Offset, Block));
    hdf5::Dimensions DimsMem;
    DimsMem.resize(Dims.size());
    for (size_t I = 0; I < Dims.size(); ++I) {
      DimsMem.at(I) = Dims.at(I);
    }
    DimsMem.at(0) = 1;
    auto SpaceMem = hdf5::dataspace::Simple(DimsMem, DimsMem);
    hdf5::property::DatasetCreationList DCPL;
    hdf5::Dimensions DimsOut;
    hdf5::dataspace::Simple SpaceOut;
    {
      DimsOut.resize(DimsMem.size() - 1);
      for (size_t I = 1; I < DimsMem.size(); ++I) {
        DimsOut.at(I - 1) = DimsMem.at(I);
      }
      SpaceOut = hdf5::dataspace::Simple(DimsOut, DimsOut);
    }
    bool found = false;
    try {
      LOG(Sev::Debug, "try to get latest dataset");
      Dataset.link().parent().get_dataset("data");
      found = true;
    } catch (...) {
      LOG(Sev::Debug, "cannot get latest dataset");
    }
    if (!found) {
      LOG(Sev::Debug, "Dataset \"data\" not yet present");
      for (size_t I = 0; I < DimsOut.size(); ++I) {
        LOG(Sev::Debug, "I: {}: {}", I, DimsOut.at(I));
      }
      Dataset.link().parent().create_dataset("data", Type, SpaceOut, DCPL);
      Dataset.link().file().flush(hdf5::file::Scope::GLOBAL);
      LOG(Sev::Debug, "Dataset \"data\" created");
    }
    auto Latest = Dataset.link().parent().get_dataset("data");
    if (Dims.at(0) > 0) {
      std::vector<DataType> Buffer;
      size_t N = 1;
      for (size_t I = 0; I < DimsMem.size(); ++I) {
        N *= DimsMem.at(I);
      }
      Buffer.resize(N);
      Dataset.read(Buffer, Type, SpaceMem, SpaceIn);
      size_t S = 0;
      for (size_t I = 0; I < Buffer.size(); ++I) {
        S += Buffer.at(I);
      }
      LOG(Sev::Debug, "copy latest histogram.  sum: {}", S);
      Latest.write(Buffer, Type, SpaceMem, SpaceOut);
    } else {
      LOG(Sev::Debug, "No entries so far");
    }
  }
  return 0;
}

template <typename DataType, typename EdgeType, typename ErrorType>
typename WriterTyped<DataType, EdgeType, ErrorType>::ptr
WriterTyped<DataType, EdgeType, ErrorType>::createFromJson(json const &Json) {
  if (!Json.is_object()) {
    throw UnexpectedJsonInput();
  }
  auto TheWriterTypedPtr =
      make_unique<WriterTyped<DataType, EdgeType, ErrorType>>();
  auto &TheWriterTyped = *TheWriterTypedPtr;
  try {
    TheWriterTyped.TheShape = Shape<EdgeType>::createFromJson(Json.at("shape"));
    try {
      TheWriterTyped.DoConvertEdgeTypeToFloat =
          Json.at("convert_edge_type_to_float");
    } catch (json::out_of_range const &) {
    }
    TheWriterTyped.CreatedFromJson = Json.dump();
  } catch (json::out_of_range const &) {
    std::throw_with_nested(UnexpectedJsonInput());
  }
  return TheWriterTypedPtr;
}

template <typename DataType, typename EdgeType, typename ErrorType>
typename WriterTyped<DataType, EdgeType, ErrorType>::ptr
WriterTyped<DataType, EdgeType, ErrorType>::createFromHDF(
    hdf5::node::Group &Group) {
  std::string JsonString;
  Group.attributes["created_from_json"].read(JsonString);
  auto TheWriterTypedPtr =
      WriterTyped<DataType, EdgeType, ErrorType>::createFromJson(
          json::parse(JsonString));
  auto &TheWriterTyped = *TheWriterTypedPtr;
  TheWriterTyped.Dataset = Group.get_dataset("histograms");
  TheWriterTyped.DatasetErrors = Group.get_dataset("errors");
  TheWriterTyped.DatasetTimestamps = Group.get_dataset("timestamps");
  TheWriterTyped.DatasetInfo = Group.get_dataset("info");
  TheWriterTyped.DatasetInfoTimestamp = Group.get_dataset("info_timestamp");
  return TheWriterTypedPtr;
}

template <typename DataType, typename EdgeType, typename ErrorType>
void WriterTyped<DataType, EdgeType, ErrorType>::createHDFStructure(
    hdf5::node::Group &Group, size_t ChunkBytes) {
  Group.attributes.create_from("created_from_json", CreatedFromJson);
  this->ChunkBytes = ChunkBytes;
  auto Type = hdf5::datatype::create<DataType>().native_type();
  hdf5::dataspace::Simple Space;
  {
    auto const &Dims = TheShape.getDimensions();
    std::vector<hsize_t> SizeNow{0};
    std::vector<hsize_t> SizeMax{H5S_UNLIMITED};
    for (auto const &Dim : Dims) {
      SizeNow.push_back(Dim.getSize());
      SizeMax.push_back(Dim.getSize());
    }
    Space = hdf5::dataspace::Simple(SizeNow, SizeMax);
  }
  hdf5::property::DatasetCreationList DCPL;
  {
    auto Dims = Space.maximum_dimensions();
    std::vector<hsize_t> ChunkElements(Dims.size());
    ChunkElements.at(0) = ChunkBytes / Type.size();
    for (size_t i = 1; i < Dims.size(); ++i) {
      ChunkElements.at(i) = Dims.at(i);
      ChunkElements.at(0) /= Dims.at(i);
    }
    if (ChunkElements.at(0) == 0) {
      ChunkElements.at(0) = 1;
    }
    DCPL.chunk(ChunkElements);
  }
  {
    if (0 > H5Pset_deflate(static_cast<hid_t>(DCPL), 7)) {
      LOG(Sev::Critical, "can not use gzip filter on hdf5. Is hdf5 not built "
                         "with gzip support?");
    }
  }
  Dataset = Group.create_dataset("histograms", Type, Space, DCPL);
  copyLatestToData(0);
  DatasetErrors = Group.create_dataset(
      "errors", hdf5::datatype::create<ErrorType>().native_type(), Space, DCPL);
  {
    hdf5::property::DatasetCreationList DCPL;
    DCPL.chunk({4 * 1024, 2});
    auto Space = hdf5::dataspace::Simple({0, 2}, {H5S_UNLIMITED, 2});
    auto Type = hdf5::datatype::create<uint64_t>().native_type();
    DatasetTimestamps = Group.create_dataset("timestamps", Type, Space, DCPL);
  }
  {
    hdf5::property::DatasetCreationList DCPL;
    DCPL.chunk({4 * 1024});
    auto Space = hdf5::dataspace::Simple({0}, {H5S_UNLIMITED});
    auto Type = hdf5::datatype::create<uint64_t>().native_type();
    DatasetInfoTimestamp =
        Group.create_dataset("info_timestamp", Type, Space, DCPL);
  }
  {
    hdf5::property::DatasetCreationList DCPL;
    DCPL.chunk({4 * 1024});
    auto Space = hdf5::dataspace::Simple({0}, {H5S_UNLIMITED});
    auto Type = hdf5::datatype::String(
        hdf5::datatype::create<std::string>().native_type());
    Type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    DatasetInfo = Group.create_dataset("info", Type, Space, DCPL);
  }
  uint64_t DimId = 0;
  for (auto const &Dim : TheShape.getDimensions()) {
    hdf5::property::DatasetCreationList DCPL;
    auto SpaceFile =
        hdf5::dataspace::Simple({Dim.getSize() + 1}, {Dim.getSize() + 1});
    auto SpaceMem =
        hdf5::dataspace::Simple({Dim.getSize() + 1}, {Dim.getSize() + 1});
    auto TypeMem = hdf5::datatype::create<EdgeType>().native_type();
    auto TypeSpace = TypeMem;
    if (DoConvertEdgeTypeToFloat) {
      TypeSpace = hdf5::datatype::create<float>().native_type();
    }
    auto Dataset =
        Group.create_dataset(Dim.getDatasetName(), TypeSpace, SpaceFile, DCPL);
    Dataset.write(Dim.getEdges(), TypeMem, SpaceMem, SpaceFile);
    hdf5::property::AttributeCreationList ACPL;
    Dataset.attributes.create_from("axis", 1 + DimId);
    {
      auto TypeUTF8 = hdf5::datatype::String(
          hdf5::datatype::create<std::string>().native_type());
      TypeUTF8.encoding(hdf5::datatype::CharacterEncoding::UTF8);
      auto SpaceMem = hdf5::dataspace::Simple({1}, {1});
      Dataset.attributes.create("units", TypeUTF8, SpaceMem)
          .write(Dim.getUnit());
    }
    ++DimId;
  }
}

template <typename DataType> Array getMatchingFlatbufferType(DataType *);

template <typename DataType, typename EdgeType, typename ErrorType>
HDFWriterModule::WriteResult WriterTyped<DataType, EdgeType, ErrorType>::write(
    FlatbufferMessage const &Message, bool DoFlushEachWrite) {
  if (!Dataset.is_valid()) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("invalid dataset");
  }
  auto Dims = hdf5::dataspace::Simple(Dataset.dataspace()).current_dimensions();
  if (Dims.size() < 1) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("Dims.size() < 1");
  }
  auto EvMsg = GetEventHistogram(Message.data());
  uint64_t Timestamp = EvMsg->timestamp();
  if (Timestamp == 0) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("Timestamp == 0");
  }
  if (EvMsg->data_type() != getMatchingFlatbufferType<DataType>(nullptr)) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "EvMsg->data_type() != getMatchingFlatbufferType<DataType>(nullptr)");
  }
  auto DataUnion = static_cast<FlatbufferDataType const *>(EvMsg->data());
  if (!DataUnion) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("!DataUnion");
  }
  auto DataPtr = DataUnion->value();
  if (!DataPtr) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("!DataPtr");
  }
  flatbuffers::Vector<ErrorType> const *ErrorsPtr = nullptr;
  auto ErrorsUnion = static_cast<FlatbufferErrorType const *>(EvMsg->errors());
  if (ErrorsUnion) {
    if (EvMsg->errors_type() != getMatchingFlatbufferType<ErrorType>(nullptr)) {
      return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
          "EvMsg->errors_type() != "
          "getMatchingFlatbufferType<ErrorType>(nullptr)");
    }
    ErrorsPtr = ErrorsUnion->value();
  }
  auto MsgShape = EvMsg->current_shape();
  if (!MsgShape) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "Missing current_shape");
  }
  if (MsgShape->size() != Dims.size() - 1) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "Wrong size of shape");
  }
  std::vector<uint32_t> TheOffsets;
  TheOffsets.resize(MsgShape->size());
  auto MsgOffset = EvMsg->offset();
  if (MsgOffset) {
    if (MsgOffset->size() == MsgShape->size()) {
      for (size_t I = 0; I < TheOffsets.size(); ++I) {
        auto X = MsgOffset->data()[I];
        if (X + MsgShape->data()[I] > Dims.at(I + 1)) {
          return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
              "Shape not consistent");
        }
        TheOffsets.at(I) = X;
      }
    }
  }
  size_t ExpectedLinearDataSize = 1;
  for (size_t i = 0; i < MsgShape->size(); ++i) {
    ExpectedLinearDataSize *= MsgShape->data()[i];
  }
  if (ExpectedLinearDataSize != DataPtr->size()) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "Unexpected data size");
  }
  if (ErrorsPtr) {
    if (ExpectedLinearDataSize != ErrorsPtr->size()) {
      return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
          "Unexpected errors size");
    }
  }
  if (Timestamp + 40l * 1000000000l < LargestTimestampSeen) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        fmt::format("Old histogram: Timestamp: {}  LargestTimestampSeen: {}",
                    Timestamp, LargestTimestampSeen));
  }
  if (LargestTimestampSeen < Timestamp) {
    LargestTimestampSeen = Timestamp;
  }
  bool AddNewRow = false;
  if (HistogramRecords.find(Timestamp) == HistogramRecords.end()) {
    if (HistogramRecords.size() >= MaxNumberHistoric) {
      auto ReuseHDFIndex = HistogramRecords.begin()->second.getHDFIndex();
      HistogramRecords.erase(HistogramRecords.begin());
      HistogramRecords[Timestamp] =
          HistogramRecord::create(ReuseHDFIndex, TheShape.getTotalItems());
    } else {
      HistogramRecords[Timestamp] =
          HistogramRecord::create(Dims.at(0), TheShape.getTotalItems());
      AddNewRow = true;
    }
  }
  if (AddNewRow) {
    Dims.at(0) += 1;
    Dataset.extent(Dims);
    DatasetErrors.extent(Dims);
  }
  auto TheSlice = Slice::fromOffsetsSizes(
      std::vector<uint32_t>(TheOffsets.data(),
                            TheOffsets.data() + TheOffsets.size()),
      std::vector<uint32_t>(MsgShape->data(),
                            MsgShape->data() + MsgShape->size()));
  auto &Record = HistogramRecords[Timestamp];
  if (!Record.hasEmptySlice(TheSlice)) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "Slice already at least partially filled");
  }
  Record.addSlice(TheSlice);
  hdf5::dataspace::Simple DSPMem;
  auto DSPFile = Dataset.dataspace();
  {
    std::vector<hsize_t> Offset(Dims.size());
    Offset.at(0) = Record.getHDFIndex();
    std::vector<hsize_t> Block(Dims.size());
    Block.at(0) = 1;
    auto Count = Dims;
    auto Stride = Dims;
    for (size_t i = 0; i < Count.size(); ++i) {
      Count.at(i) = 1;
      Stride.at(i) = 1;
    }
    for (size_t i = 1; i < Count.size(); ++i) {
      Offset.at(i) = TheOffsets.at(i - 1);
      Block.at(i) = MsgShape->data()[i - 1];
    }
    DSPFile.selection(hdf5::dataspace::SelectionOperation::SET,
                      hdf5::dataspace::Hyperslab(Offset, Block, Count, Stride));
    DSPMem = hdf5::dataspace::Simple(Block, Block);
  }
  Dataset.write(*DataPtr->data(),
                hdf5::datatype::create<DataType>().native_type(), DSPMem,
                DSPFile);
  if (ErrorsPtr) {
    DatasetErrors.write(*ErrorsPtr->data(),
                        hdf5::datatype::create<ErrorType>().native_type(),
                        DSPMem, DSPFile);
  }
  Record.addToItemsWritten(DataPtr->size());
  {
    std::vector<uint64_t> Timestamps;
    Timestamps.resize(2 *
                      hdf5::dataspace::Simple(DatasetTimestamps.dataspace())
                          .current_dimensions()
                          .at(0));
    DatasetTimestamps.read(Timestamps, hdf5::property::DatasetTransferList());
    Timestamps.resize(2 * HistogramRecords.size());
    Timestamps.at(2 * Record.getHDFIndex()) = Timestamp;
    if (Record.isFull()) {
      Timestamps.at(2 * Record.getHDFIndex() + 1) = 1;
    }
    DatasetTimestamps.extent({Timestamps.size() / 2, 2});
    DatasetTimestamps.write(Timestamps);
  }

  if (EvMsg->info()) {
    {
      auto TypeMem = hdf5::datatype::String(
          hdf5::datatype::create<std::string>().native_type());
      TypeMem.encoding(hdf5::datatype::CharacterEncoding::UTF8);
      auto DSPFile = hdf5::dataspace::Simple(DatasetInfo.dataspace());
      auto Dims = DSPFile.current_dimensions();
      Dims.at(0) += 1;
      DatasetInfo.extent(Dims);
      DSPFile = hdf5::dataspace::Simple(DatasetInfo.dataspace());
      DSPFile.selection(
          hdf5::dataspace::SelectionOperation::SET,
          hdf5::dataspace::Hyperslab({Dims.at(0) - 1}, {1}, {1}, {1}));
      DSPMem = hdf5::dataspace::Simple({1}, {1});
      DatasetInfo.write(std::vector<std::string>({EvMsg->info()->str()}),
                        TypeMem, DSPMem, DSPFile);
    }
    {
      auto DSPFile = hdf5::dataspace::Simple(DatasetInfoTimestamp.dataspace());
      auto Dims = DSPFile.current_dimensions();
      Dims.at(0) += 1;
      DatasetInfoTimestamp.extent(Dims);
      DSPFile = hdf5::dataspace::Simple(DatasetInfo.dataspace());
      DSPFile.selection(
          hdf5::dataspace::SelectionOperation::SET,
          hdf5::dataspace::Hyperslab({Dims.at(0) - 1}, {1}, {1}, {1}));
      auto DSPMem = hdf5::dataspace::Simple({1}, {1});
      auto TypeMem = hdf5::datatype::create<uint64_t>().native_type();
      DatasetInfoTimestamp.write(Timestamp, TypeMem, DSPMem, DSPFile);
    }
  }

  if (Record.isFull()) {
    copyLatestToData(Record.getHDFIndex());
  }

  if (DoFlushEachWrite) {
    Dataset.link().file().flush(hdf5::file::Scope::GLOBAL);
  }
  LOG(Sev::Debug, "hs00 -------------------------------   DONE");
  return HDFWriterModule::WriteResult::OK();
}
}
}
}
