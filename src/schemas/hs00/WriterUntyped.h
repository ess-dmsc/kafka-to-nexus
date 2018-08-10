#pragma once

#include "../../HDFWriterModule.h"
#include "Shape.h"
#include "json.h"
#include <h5cpp/hdf5.hpp>
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

class WriterUntyped {
public:
  using ptr = std::unique_ptr<WriterUntyped>;
  using json = nlohmann::json;
  /// Create a WriterTyped from Json, used when a new write command arrives at
  /// the file writer.
  static ptr createFromJson(json const &Json);

  /// Create the Writer during HDF reopen
  static ptr createFromHDF(hdf5::node::Group &Group);

  /// Create the HDF structures. used on arrival of a new write command for the
  /// file writer to create the initial structure of the HDF file for this
  /// writer module.
  virtual void createHDFStructure(hdf5::node::Group &Group,
                                  size_t ChunkBytes) = 0;

  virtual HDFWriterModule::WriteResult write(Msg const &msg,
                                             bool DoFlushEachWrite) = 0;

private:
  template <typename DataType> static ptr createFromJsonL1(json const &Json);

  template <typename DataType, typename EdgeType>
  static ptr createFromJsonL2(json const &Json);

  template <typename DataType>
  static WriterUntyped::ptr createFromHDFWithDataType(hdf5::node::Group &Group,
                                                      json const &Json);

  template <typename DataType, typename EdgeType>
  static WriterUntyped::ptr
  createFromHDFWithDataTypeAndEdgeType(hdf5::node::Group &Group,
                                       json const &Json);
};
}
}
}
