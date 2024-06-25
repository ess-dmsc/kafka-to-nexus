// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Exceptions.h"
#include "FlatbufferMessage.h"
#include "HistogramRecord.h"
#include "Shape.h"
#include "WriterUntyped.h"
#include "helper.h"
#include "json.h"
#include "logger.h"
#include <flatbuffers/flatbuffers.h>
#include <h5cpp/hdf5.hpp>
#include <type_traits>
#include <vector>

namespace WriterModule {
namespace hs00 {

#include "hs00_event_histogram_generated.h"

template <typename DataType, typename EdgeType, typename ErrorType>
class WriterTyped : public WriterUntyped {
private:
  using json = nlohmann::json;

public:
  using ptr = std::unique_ptr<WriterTyped<DataType, EdgeType, ErrorType>>;

  /// \brief Create a WriterTyped from Json, used when a new write command
  /// arrives at the file writer.
  static ptr create(json const &Json);

  static ptr reOpen(hdf5::node::Group &Group);

  /// \brief Create the HDF structures.
  ///
  /// Used on arrival of a new write command for the
  /// file writer to create the initial structure of the HDF file for this
  /// writer module.
  ///
  /// \param Group
  /// \param ChunkSize
  void createHDFStructure(hdf5::node::Group &Group, size_t ChunkSize) override;

  void write(FlatbufferMessage const &Message) override;

  ~WriterTyped() override;
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

  uint64_t LargestTimestampSeen = 0;
  size_t MaxNumberHistoric = 4;
};

template <typename DataType, typename EdgeType, typename ErrorType>
WriterTyped<DataType, EdgeType, ErrorType>::~WriterTyped() {
  LOG_TRACE("WriterTyped destructor");
}

template <typename DataType, typename EdgeType, typename ErrorType>
int WriterTyped<DataType, EdgeType, ErrorType>::copyLatestToData(
    size_t HDFIndex) {
  LOG_TRACE("WriterTyped copyLatestToData");
  if (Dataset.is_valid()) {
    LOG_TRACE("Found valid dataset");
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
    SpaceIn.selection(hdf5::dataspace::SelectionOperation::Set,
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
      LOG_TRACE("Attempting to get latest dataset");
      Dataset.link().parent().get_dataset("data");
      found = true;
    } catch (...) {
      LOG_TRACE("Failed to get latest dataset");
    }
    if (!found) {
      LOG_TRACE(R"(Dataset "data" not yet present)");
      for (size_t I = 0; I < DimsOut.size(); ++I) {
        LOG_TRACE("I: {}: {}", I, DimsOut.at(I));
      }
      Dataset.link().parent().create_dataset("data", Type, SpaceOut, DCPL);
      Dataset.link().file().flush(hdf5::file::Scope::Global);
      LOG_TRACE(R"(Dataset "data" created)");
    }
    auto Latest = Dataset.link().parent().get_dataset("data");
    if (Dims.at(0) > 0) {
      size_t N = std::accumulate(DimsMem.cbegin(), DimsMem.cend(), size_t(1),
                                 std::multiplies<>());
      std::vector<DataType> Buffer(N);
      Dataset.read(Buffer, Type, SpaceMem, SpaceIn);
      size_t S = 0;
      for (size_t I = 0; I < Buffer.size(); ++I) {
        S += Buffer.at(I);
      }
      LOG_TRACE("copy latest histogram.  sum: {}", S);
      Latest.write(Buffer, Type, SpaceMem, SpaceOut);
    } else {
      LOG_TRACE("No entries so far");
    }
  }
  return 0;
}

template <typename DataType, typename EdgeType, typename ErrorType>
typename WriterTyped<DataType, EdgeType, ErrorType>::ptr
WriterTyped<DataType, EdgeType, ErrorType>::create(json const &Json) {
  if (!Json.is_object()) {
    throw UnexpectedJsonInput();
  }
  auto TheWriterTypedPtr =
      std::make_unique<WriterTyped<DataType, EdgeType, ErrorType>>();
  try {
    auto &TheWriterTyped = *TheWriterTypedPtr;
    TheWriterTyped.TheShape = Shape<EdgeType>::createFromJson(Json.at("shape"));
    TheWriterTyped.CreatedFromJson = Json.dump();
  } catch (json::out_of_range const &) {
    std::throw_with_nested(UnexpectedJsonInput());
  }
  return TheWriterTypedPtr;
}

template <typename DataType, typename EdgeType, typename ErrorType>
typename WriterTyped<DataType, EdgeType, ErrorType>::ptr
WriterTyped<DataType, EdgeType, ErrorType>::reOpen(hdf5::node::Group &Group) {
  std::string JsonString;
  Group.attributes["created_from_json"].read(JsonString);
  auto TheWriterTypedPtr = WriterTyped<DataType, EdgeType, ErrorType>::create(
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
    hdf5::node::Group &Group, size_t ChunkSize) {
  Group.attributes.create_from("created_from_json", CreatedFromJson);
  {
    auto Type = hdf5::datatype::create<DataType>().native_type();
    auto const &Dims = TheShape.getDimensions();
    std::vector<hsize_t> SizeNow{0};
    std::vector<hsize_t> SizeMax{H5S_UNLIMITED};
    for (auto const &Dim : Dims) {
      SizeNow.push_back(Dim.getSize());
      SizeMax.push_back(Dim.getSize());
    }
    auto Space = hdf5::dataspace::Simple(SizeNow, SizeMax);

    hdf5::property::DatasetCreationList DCPL;
    auto MaxDims = Space.maximum_dimensions();
    std::vector<hsize_t> ChunkElements(MaxDims.size());
    ChunkElements.at(0) = ChunkSize;
    for (size_t i = 1; i < MaxDims.size(); ++i) {
      ChunkElements.at(i) = MaxDims.at(i);
      ChunkElements.at(0) /= MaxDims.at(i);
    }
    if (ChunkElements.at(0) == 0) {
      ChunkElements.at(0) = 1;
    }
    DCPL.chunk(ChunkElements);
    if (0 > H5Pset_deflate(static_cast<hid_t>(DCPL), 7)) {
      LOG_ERROR("can not use gzip filter on hdf5. Is hdf5 not built "
                "with gzip support?");
    }
    Dataset = Group.create_dataset("histograms", Type, Space, DCPL);
    copyLatestToData(0);
    DatasetErrors = Group.create_dataset(
        "errors", hdf5::datatype::create<ErrorType>().native_type(), Space,
        DCPL);
  }
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
    auto Dataset =
        Group.create_dataset(Dim.getDatasetName(), TypeSpace, SpaceFile, DCPL);
    Dataset.write(Dim.getEdges(), TypeMem, SpaceMem, SpaceFile);
    hdf5::property::AttributeCreationList ACPL;
    Dataset.attributes.create_from("axis", 1 + DimId);
    {
      auto TypeUTF8 = hdf5::datatype::String(
          hdf5::datatype::create<std::string>().native_type());
      TypeUTF8.encoding(hdf5::datatype::CharacterEncoding::UTF8);
      auto CurrentSpaceMem = hdf5::dataspace::Scalar();
      Dataset.attributes.create("units", TypeUTF8, CurrentSpaceMem)
          .write(Dim.getUnit());
      if (Group.attributes.exists("NX_class")) {
        LOG_INFO("NX_class already specified!");
      } else {
        auto ClassAttribute = Group.attributes.create<std::string>("NX_class");
        ClassAttribute.write("NXdata");
      }
    }
    ++DimId;
  }
}

template <typename DataType> Array getMatchingFlatbufferType(DataType *);

template <typename DataType, typename EdgeType, typename ErrorType>
void WriterTyped<DataType, EdgeType, ErrorType>::write(
    FlatbufferMessage const &Message) {
  using WriterModule::WriterException;

  if (!Dataset.is_valid()) {
    throw WriterException("Invalid dataset");
  }
  auto Dims = hdf5::dataspace::Simple(Dataset.dataspace()).current_dimensions();
  if (Dims.empty()) {
    throw WriterException("Dims is empty");
  }
  auto EvMsg = GetEventHistogram(Message.data());
  uint64_t Timestamp = EvMsg->timestamp();
  if (Timestamp == 0) {
    throw WriterException("Timestamp == 0");
  }
  if (EvMsg->data_type() != getMatchingFlatbufferType<DataType>(nullptr)) {
    throw WriterException(
        "EvMsg->data_type() != getMatchingFlatbufferType<DataType>(nullptr)");
  }
  auto DataUnion = static_cast<FlatbufferDataType const *>(EvMsg->data());
  if (!DataUnion) {
    throw WriterException("!DataUnion");
  }
  auto DataPtr = DataUnion->value();
  if (!DataPtr) {
    throw WriterException("!DataPtr");
  }
  flatbuffers::Vector<ErrorType> const *ErrorsPtr = nullptr;
  auto ErrorsUnion = static_cast<FlatbufferErrorType const *>(EvMsg->errors());
  if (ErrorsUnion) {
    if (EvMsg->errors_type() != getMatchingFlatbufferType<ErrorType>(nullptr)) {
      throw WriterException("EvMsg->errors_type() != "
                            "getMatchingFlatbufferType<ErrorType>(nullptr)");
    }
    ErrorsPtr = ErrorsUnion->value();
  }
  auto MsgShape = EvMsg->current_shape();
  if (!MsgShape) {
    throw WriterException("Missing current_shape");
  }
  if (MsgShape->size() != Dims.size() - 1) {
    throw WriterException("Wrong size of shape");
  }
  std::vector<uint32_t> TheOffsets;
  TheOffsets.resize(MsgShape->size());
  auto MsgOffset = EvMsg->offset();
  if (MsgOffset) {
    if (MsgOffset->size() == MsgShape->size()) {
      for (size_t I = 0; I < TheOffsets.size(); ++I) {
        auto X = MsgOffset->data()[I];
        if (X + MsgShape->data()[I] > Dims.at(I + 1)) {
          throw WriterException("Shape not consistent");
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
    throw WriterException("Unexpected data size");
  }
  if (ErrorsPtr) {
    if (ExpectedLinearDataSize != ErrorsPtr->size()) {
      throw WriterException("Unexpected errors size");
    }
  }
  if (Timestamp + 40l * 1000000000l < LargestTimestampSeen) {
    throw WriterException(
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
    throw WriterException("Slice already at least partially filled");
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
    DSPFile.selection(hdf5::dataspace::SelectionOperation::Set,
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
    Timestamps.resize(2 * hdf5::dataspace::Simple(DatasetTimestamps.dataspace())
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
      auto SimpleDSPFile = hdf5::dataspace::Simple(DatasetInfo.dataspace());
      auto CurrentDims = SimpleDSPFile.current_dimensions();
      CurrentDims.at(0) += 1;
      DatasetInfo.extent(CurrentDims);
      SimpleDSPFile = hdf5::dataspace::Simple(DatasetInfo.dataspace());
      SimpleDSPFile.selection(
          hdf5::dataspace::SelectionOperation::Set,
          hdf5::dataspace::Hyperslab({CurrentDims.at(0) - 1}, {1}, {1}, {1}));
      DSPMem = hdf5::dataspace::Simple({1}, {1});
      DatasetInfo.write(std::vector<std::string>({EvMsg->info()->str()}),
                        TypeMem, DSPMem, SimpleDSPFile);
    }
    {
      auto CurrentDSPFile =
          hdf5::dataspace::Simple(DatasetInfoTimestamp.dataspace());
      auto CurrentDims = CurrentDSPFile.current_dimensions();
      CurrentDims.at(0) += 1;
      DatasetInfoTimestamp.extent(CurrentDims);
      CurrentDSPFile = hdf5::dataspace::Simple(DatasetInfo.dataspace());
      CurrentDSPFile.selection(
          hdf5::dataspace::SelectionOperation::Set,
          hdf5::dataspace::Hyperslab({CurrentDims.at(0) - 1}, {1}, {1}, {1}));
      auto CurrentDSPMem = hdf5::dataspace::Simple({1}, {1});
      auto TypeMem = hdf5::datatype::create<uint64_t>().native_type();
      DatasetInfoTimestamp.write(Timestamp, TypeMem, CurrentDSPMem,
                                 CurrentDSPFile);
    }
  }

  if (Record.isFull()) {
    copyLatestToData(Record.getHDFIndex());
  }

  LOG_TRACE("hs00 -------------------------------   DONE");
}
} // namespace hs00
} // namespace WriterModule
