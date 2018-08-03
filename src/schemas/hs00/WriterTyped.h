#pragma once

#include "Shape.h"
#include "WriterUntyped.h"
#include "json.h"
#include <flatbuffers/flatbuffers.h>
#include <h5cpp/hdf5.hpp>
#include <type_traits>
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

#include "schemas/hs00_event_histogram_generated.h"

template <typename DataType, typename EdgeType>
class WriterTyped : public WriterUntyped {
private:
  using json = nlohmann::json;

public:
  using ptr = std::unique_ptr<WriterTyped>;

  /// Create a WriterTyped from Json, used when a new write command arrives at
  /// the file writer.
  static ptr createFromJson(json const &Json);

  static ptr createFromHDF(hdf5::node::Group &Group);

  /// Create the HDF structures. used on arrival of a new write command for the
  /// file writer to create the initial structure of the HDF file for this
  /// writer module.
  void createHDFStructure(hdf5::node::Group &Group, size_t ChunkBytes) override;

  HDFWriterModule::WriteResult write(Msg const &msg) override;

private:
  std::string SourceName;
  Shape<EdgeType> TheShape;
  std::string CreatedFromJson;
  hdf5::node::Dataset Dataset;

  // clang-format off
  using FlatbufferDataType =
  typename std::conditional<std::is_same<DataType, uint32_t>::value, ArrayUInt,
  typename std::conditional<std::is_same<DataType, uint64_t>::value, ArrayULong,
  typename std::conditional<std::is_same<DataType,   double>::value, ArrayDouble,
  std::nullptr_t>::type>::type>::type;
  // clang-format on
};
}
}
}
