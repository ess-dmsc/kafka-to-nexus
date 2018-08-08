#include "WriterTyped.h"
#include "../../helper.h"
#include "Exceptions.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename DataType, typename EdgeType>
typename WriterTyped<DataType, EdgeType>::ptr
WriterTyped<DataType, EdgeType>::createFromJson(json const &Json) {
  if (!Json.is_object()) {
    throw UnexpectedJsonInput();
  }
  auto TheWriterTypedPtr = make_unique<WriterTyped<DataType, EdgeType>>();
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

template <typename DataType, typename EdgeType>
typename WriterTyped<DataType, EdgeType>::ptr
WriterTyped<DataType, EdgeType>::createFromHDF(hdf5::node::Group &Group) {
  std::string JsonString;
  Group.attributes["created_from_json"].read(JsonString);
  auto TheWriterTypedPtr =
      WriterTyped<DataType, EdgeType>::createFromJson(json::parse(JsonString));
  auto &TheWriterTyped = *TheWriterTypedPtr;
  TheWriterTyped.Dataset = Group.get_dataset("histograms");
  return TheWriterTypedPtr;
}

template <typename DataType, typename EdgeType>
void WriterTyped<DataType, EdgeType>::createHDFStructure(
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
}

template WriterTyped<uint64_t, double>::ptr
WriterTyped<uint64_t, double>::createFromJson(json const &Json);

template WriterTyped<uint64_t, double>::ptr
WriterTyped<uint64_t, double>::createFromHDF(hdf5::node::Group &Group);

template void
WriterTyped<uint64_t, double>::createHDFStructure(hdf5::node::Group &Group,
                                                  size_t ChunkBytes);

template <typename DataType> Array getMatchingFlatbufferType(DataType *);
template <> Array getMatchingFlatbufferType(uint32_t *) {
  return Array::ArrayUInt;
}
template <> Array getMatchingFlatbufferType(uint64_t *) {
  return Array::ArrayULong;
}
template <> Array getMatchingFlatbufferType(double *) {
  return Array::ArrayDouble;
}

template <typename DataType, typename EdgeType>
HDFWriterModule::WriteResult
WriterTyped<DataType, EdgeType>::write(Msg const &Msg) {
  if (!Dataset.is_valid()) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("invalid dataset");
  }
  auto Dims = hdf5::dataspace::Simple(Dataset.dataspace()).current_dimensions();
  if (Dims.size() < 1) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("Dims.size() < 1");
  }
  if (Dims.at(0) == 0) {
    Dims.at(0) += 1;
  }
  Dataset.extent(Dims);
  auto EvMsg = GetEventHistogram(Msg.data());
  uint64_t Timestamp = EvMsg->timestamp();
  if (Timestamp == 0) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("Timestamp == 0");
  }
  if (EvMsg->data_type() != getMatchingFlatbufferType<DataType>(nullptr)) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "EvMsg->data_type() != getMatchingFlatbufferType<DataType>(nullptr)");
  }
  auto DataPtr =
      static_cast<FlatbufferDataType const *>(EvMsg->data())->value();
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
    HistogramRecords[Timestamp] = HistogramRecord::create();
  }
  // auto & Record = HistogramRecords[Timestamp];
  Slice::fromOffsetsSizes(
      std::vector<uint32_t>(MsgOffset->data(),
                            MsgOffset->data() + MsgOffset->size()),
      std::vector<uint32_t>(MsgShape->data(),
                            MsgShape->data() + MsgShape->size()));
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
  return HDFWriterModule::WriteResult::OK();
}
}
}
}
