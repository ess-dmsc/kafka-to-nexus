#pragma once

#include "Shape.h"
#include "json.h"
#include <h5cpp/hdf5.hpp>
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename DataType, typename EdgeType> class WriterTyped {
private:
  using json = nlohmann::json;

public:
  /// Create a WriterTyped from Json, used when a new write command arrives at
  /// the file writer.
  static WriterTyped createFromJson(json const &Json);

  /// Create the HDF structures. used on arrival of a new write command for the
  /// file writer to create the initial structure of the HDF file for this
  /// writer module.
  void createHDFStructure(hdf5::node::Group const &Group);

private:
  std::string SourceName;
  Shape<EdgeType> TheShape;
};
}
}
}
