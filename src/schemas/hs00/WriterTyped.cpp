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
    std::vector<hsize_t> SizeNow{0};
    std::vector<hsize_t> SizeMax{H5S_UNLIMITED};
    Space = hdf5::dataspace::Simple(SizeNow, SizeMax);
  }
  hdf5::property::DatasetCreationList DCPL;
  DCPL.chunk({std::max<hsize_t>(1, ChunkBytes / Type.size())});
  Dataset = Group.create_dataset("histograms", Type, Space, DCPL);
}

template WriterTyped<uint64_t, double>::ptr
WriterTyped<uint64_t, double>::createFromJson(json const &Json);

template WriterTyped<uint64_t, double>::ptr
WriterTyped<uint64_t, double>::createFromHDF(hdf5::node::Group &Group);

template void
WriterTyped<uint64_t, double>::createHDFStructure(hdf5::node::Group &Group,
                                                  size_t ChunkBytes);

template <typename DataType, typename EdgeType>
HDFWriterModule::WriteResult
WriterTyped<DataType, EdgeType>::write(FlatbufferMessage const &Message) {
  return HDFWriterModule::WriteResult::OK();
}
}
}
}
