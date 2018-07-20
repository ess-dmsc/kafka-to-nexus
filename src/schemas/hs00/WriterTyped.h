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

  HDFWriterModule::WriteResult write(FlatbufferMessage const &Message) override;

private:
  std::string SourceName;
  Shape<EdgeType> TheShape;
  std::string CreatedFromJson;
  hdf5::node::Dataset Dataset;
  hdf5::node::Dataset DatasetTimestamps;

  // clang-format off
  using FlatbufferDataType =
  typename std::conditional<std::is_same<DataType, uint32_t>::value, ArrayUInt,
  typename std::conditional<std::is_same<DataType, uint64_t>::value, ArrayULong,
  typename std::conditional<std::is_same<DataType,   double>::value, ArrayDouble,
  std::nullptr_t>::type>::type>::type;

  using FlatbufferErrorType =
  typename std::conditional<std::is_same<ErrorType, uint32_t>::value, ArrayUInt,
  typename std::conditional<std::is_same<ErrorType, uint64_t>::value, ArrayULong,
  typename std::conditional<std::is_same<ErrorType,   double>::value, ArrayDouble,
  std::nullptr_t>::type>::type>::type;
  // clang-format on

  std::map<uint64_t, HistogramRecord> HistogramRecords;
  std::vector<HistogramRecord> HistogramRecordsFreed;
};

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
    TheWriterTyped.SourceName = Json.at("source_name");
    TheWriterTyped.TheShape = Shape<EdgeType>::createFromJson(Json.at("shape"));
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
  TheWriterTyped.DatasetTimestamps = Group.get_dataset("timestamps");
  return TheWriterTypedPtr;
}

template <typename DataType, typename EdgeType, typename ErrorType>
void WriterTyped<DataType, EdgeType, ErrorType>::createHDFStructure(
    hdf5::node::Group &Group, size_t ChunkBytes) {
  Group.attributes.create_from("created_from_json", CreatedFromJson);
  auto Type = hdf5::datatype::create<DataType>().native_type();
  hdf5::dataspace::Simple Space;
  {
    auto const &Dims = TheShape.getDimensions();
    std::vector<hsize_t> SizeNow{0};
    std::vector<hsize_t> SizeMax{H5S_UNLIMITED};
    for (auto Dim : Dims) {
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
  Dataset = Group.create_dataset("histograms", Type, Space, DCPL);
  {
    hdf5::property::DatasetCreationList DCPL;
    DCPL.chunk({4 * 1024, 2});
    auto Space = hdf5::dataspace::Simple({0, 2}, {H5S_UNLIMITED, 2});
    auto Type = hdf5::datatype::create<uint64_t>().native_type();
    DatasetTimestamps = Group.create_dataset("timestamps", Type, Space, DCPL);
  }
}

template <typename DataType> Array getMatchingFlatbufferType(DataType *);

template <typename DataType, typename EdgeType, typename ErrorType>
HDFWriterModule::WriteResult WriterTyped<DataType, EdgeType, ErrorType>::write(
    FlatbufferMessage const &Message) {
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
  if (!EvMsg->data()) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("!EvMsg->data()");
  }
  auto DataUnion = static_cast<FlatbufferDataType const *>(EvMsg->data());
  if (!DataUnion) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("!DataUnion");
  }
  auto DataPtr = DataUnion->value();
  if (!EvMsg->errors()) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("!EvMsg->errors()");
  }
  auto ErrorsUnion = static_cast<FlatbufferErrorType const *>(EvMsg->errors());
  if (!ErrorsUnion) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("!ErrorsUnion");
  }
  // auto ErrorsPtr = ErrorsUnion->value();
  auto MsgShape = EvMsg->current_shape();
  auto MsgOffset = EvMsg->offset();
  if (!MsgShape) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "Missing current_shape");
  }
  if (!MsgOffset) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("Missing offset");
  }
  if (MsgShape->size() != Dims.size() - 1) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "Wrong size of shape");
  }
  if (MsgOffset->size() != Dims.size() - 1) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "Wrong size of offset");
  }
  for (size_t i = 0; i < Dims.size() - 1; ++i) {
    if (MsgOffset->data()[i] + MsgShape->data()[i] > Dims.at(i + 1)) {
      return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
          "Shape not consistent");
    }
  }
  size_t ExpectedLinearDataSize = 1;
  for (size_t i = 0; i < MsgShape->size(); ++i) {
    ExpectedLinearDataSize *= MsgShape->data()[i];
  }
  if (ExpectedLinearDataSize != DataPtr->size()) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "Unexpected payload size");
  }
  if (HistogramRecords.find(Timestamp) == HistogramRecords.end()) {
    HistogramRecords[Timestamp] =
        HistogramRecord::create(Dims.at(0), TheShape.getTotalItems());
    Dims.at(0) += 1;
    Dataset.extent(Dims);
  }
  auto &Record = HistogramRecords[Timestamp];
  auto TheSlice = Slice::fromOffsetsSizes(
      std::vector<uint32_t>(MsgOffset->data(),
                            MsgOffset->data() + MsgOffset->size()),
      std::vector<uint32_t>(MsgShape->data(),
                            MsgShape->data() + MsgShape->size()));
  if (!Record.hasEmptySlice(TheSlice)) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "Slice already at least partially filled");
  }
  Record.addSlice(TheSlice);
  hdf5::dataspace::Simple DSPMem;
  auto DSPFile = Dataset.dataspace();
  {
    std::vector<hsize_t> Offset(Dims.size());
    Offset.at(0) = Dims.at(0) - 1;
    std::vector<hsize_t> Block(Dims.size());
    Block.at(0) = 1;
    auto Count = Dims;
    auto Stride = Dims;
    for (size_t i = 0; i < Count.size(); ++i) {
      Count.at(i) = 1;
      Stride.at(i) = 1;
    }
    for (size_t i = 1; i < Count.size(); ++i) {
      Offset.at(i) = MsgOffset->data()[i - 1];
      Block.at(i) = MsgShape->data()[i - 1];
    }
    DSPFile.selection(hdf5::dataspace::SelectionOperation::SET,
                      hdf5::dataspace::Hyperslab(Offset, Block, Count, Stride));
    DSPMem = hdf5::dataspace::Simple(Block, Block);
  }
  Dataset.write(*DataPtr->data(),
                hdf5::datatype::create<DataType>().native_type(), DSPMem,
                DSPFile);
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
  Dataset.link().file().flush(hdf5::file::Scope::GLOBAL);
  return HDFWriterModule::WriteResult::OK();
}
}
}
}
