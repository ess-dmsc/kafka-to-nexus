#include "WriterUntyped.h"
#include "../../logger.h"
#include "Exceptions.h"
#include "WriterTyped.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename DataType>
WriterUntyped::ptr WriterUntyped::createFromJsonL1(json const &Json) {
  if (Json.at("edge_type") == "double") {
    return WriterUntyped::createFromJsonL2<DataType, double>(Json);
  } else {
    throw std::runtime_error(
        fmt::format("unimplemented edge_type: {}",
                    Json.at("data_type").get<std::string>()));
  }
}

template <typename DataType, typename EdgeType>
WriterUntyped::ptr WriterUntyped::createFromJsonL2(json const &Json) {
  typename WriterTyped<DataType, EdgeType>::ptr TheWriterTyped;
  TheWriterTyped = WriterTyped<DataType, EdgeType>::createFromJson(Json);
  return TheWriterTyped;
}

WriterUntyped::ptr WriterUntyped::createFromJson(json const &Json) {
  if (Json.at("data_type") == "uint64") {
    return WriterUntyped::createFromJsonL1<uint64_t>(Json);
  } else {
    throw std::runtime_error(
        fmt::format("unimplemented edge_type: {}",
                    Json.at("edge_type").get<std::string>()));
  }
}

/// Create the Writer during HDF reopen
WriterUntyped::ptr WriterUntyped::createFromHDF(hdf5::node::Group &Group) {
  throw unimplemented();
}
}
}
}
