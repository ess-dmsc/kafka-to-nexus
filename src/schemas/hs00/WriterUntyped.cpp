#include "WriterUntyped.h"
#include "../../logger.h"
#include "Exceptions.h"
#include "WriterTyped.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

WriterUntyped::ptr WriterUntyped::createFromJson(json const &Json) {
  if (Json.at("data_type") == "uint32") {
    return WriterUntyped::createFromJsonL1<uint32_t>(Json);
  } else if (Json.at("data_type") == "uint64") {
    return WriterUntyped::createFromJsonL1<uint64_t>(Json);
  } else if (Json.at("data_type") == "double") {
    return WriterUntyped::createFromJsonL1<double>(Json);
  } else {
    throw std::runtime_error(
        fmt::format("unimplemented data_type: {}",
                    Json.at("data_type").get<std::string>()));
  }
}

template <typename DataType>
WriterUntyped::ptr WriterUntyped::createFromJsonL1(json const &Json) {
  if (Json.at("edge_type") == "uint32") {
    return WriterUntyped::createFromJsonL2<DataType, uint32_t>(Json);
  } else if (Json.at("edge_type") == "uint64") {
    return WriterUntyped::createFromJsonL2<DataType, uint64_t>(Json);
  } else if (Json.at("edge_type") == "double") {
    return WriterUntyped::createFromJsonL2<DataType, double>(Json);
  } else {
    throw std::runtime_error(
        fmt::format("unimplemented edge_type: {}",
                    Json.at("edge_type").get<std::string>()));
  }
}

template <typename DataType, typename EdgeType>
WriterUntyped::ptr WriterUntyped::createFromJsonL2(json const &Json) {
  if (Json.at("error_type") == "uint32") {
    return WriterTyped<DataType, EdgeType, uint32_t>::createFromJson(Json);
  } else if (Json.at("error_type") == "uint64") {
    return WriterTyped<DataType, EdgeType, uint64_t>::createFromJson(Json);
  } else if (Json.at("error_type") == "double") {
    return WriterTyped<DataType, EdgeType, double>::createFromJson(Json);
  } else {
    throw std::runtime_error(
        fmt::format("unimplemented error_type: {}",
                    Json.at("error_type").get<std::string>()));
  }
}

/// Create the Writer during HDF reopen
WriterUntyped::ptr WriterUntyped::createFromHDF(hdf5::node::Group &Group) {
  std::string JsonString;
  Group.attributes["created_from_json"].read(JsonString);
  auto Json = json::parse(JsonString);
  if (Json.at("data_type") == "uint32") {
    return WriterUntyped::createFromHDFWithDataType<uint32_t>(Group, Json);
  } else if (Json.at("data_type") == "uint64") {
    return WriterUntyped::createFromHDFWithDataType<uint64_t>(Group, Json);
  } else if (Json.at("data_type") == "double") {
    return WriterUntyped::createFromHDFWithDataType<double>(Group, Json);
  } else {
    throw std::runtime_error(
        fmt::format("unimplemented data_type: {}",
                    Json.at("data_type").get<std::string>()));
  }
}

template <typename DataType>
WriterUntyped::ptr
WriterUntyped::createFromHDFWithDataType(hdf5::node::Group &Group,
                                         json const &Json) {
  // clang-format off
  if (Json.at("edge_type") == "uint32") {
    return WriterUntyped::createFromHDFWithDataTypeAndEdgeType<DataType, uint32_t>(Group, Json);
  }
  if (Json.at("edge_type") == "uint64") {
    return WriterUntyped::createFromHDFWithDataTypeAndEdgeType<DataType, uint64_t>(Group, Json);
  }
  if (Json.at("edge_type") == "double") {
    return WriterUntyped::createFromHDFWithDataTypeAndEdgeType<DataType,   double>(Group, Json);
  }
  else {
    throw std::runtime_error(fmt::format(
      "unimplemented edge_type: {}", Json.at("edge_type").get<std::string>()
    ));
  }
  // clang-format on
}

template <typename DataType, typename EdgeType>
WriterUntyped::ptr
WriterUntyped::createFromHDFWithDataTypeAndEdgeType(hdf5::node::Group &Group,
                                                    json const &Json) {
  // clang-format off
  if (Json.at("error_type") == "uint32") {
    return WriterTyped<DataType, EdgeType, uint32_t>::createFromHDF(Group);
  }
  else if (Json.at("error_type") == "uint64") {
    return WriterTyped<DataType, EdgeType, uint64_t>::createFromHDF(Group);
  }
  else if (Json.at("error_type") == "double") {
    return WriterTyped<DataType, EdgeType,   double>::createFromHDF(Group);
  }
  else {
    throw std::runtime_error(fmt::format(
      "unimplemented error_type: {}", Json.at("error_type").get<std::string>()
    ));
  }
  // clang-format on
}
}
}
}
