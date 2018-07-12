#include "WriterTyped.h"
#include "../../helper.h"
#include "Exceptions.h"
#include <flatbuffers/flatbuffers.h>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

#include "schemas/hs00_event_histogram_generated.h"

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

// Array::ArrayULong

template <typename DataType, typename EdgeType>
HDFWriterModule::WriteResult
WriterTyped<DataType, EdgeType>::write(FlatbufferMessage const &Message) {
  if (!Dataset.is_valid()) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("invalid dataset");
  }
  auto Dims = hdf5::dataspace::Simple(Dataset.dataspace()).current_dimensions();
  if (Dims.size() < 1) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE("Dims.size() < 1");
  }
  Dims.at(0) += 1;
  Dataset.extent(Dims);
  auto EvMsg = GetEventHistogram(Message.data());
  if (EvMsg->data_type() != getMatchingFlatbufferType<DataType>(nullptr)) {
    return HDFWriterModule::WriteResult::ERROR_WITH_MESSAGE(
        "EvMsg->data_type() != getMatchingFlatbufferType<DataType>(nullptr)");
  }
  EvMsg->data();
  return HDFWriterModule::WriteResult::OK();
}
}
}
}
