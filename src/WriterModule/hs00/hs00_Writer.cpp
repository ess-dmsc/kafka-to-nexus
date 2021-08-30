// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "hs00_Writer.h"
#include "Exceptions.h"
#include "WriterRegistrar.h"
#include "WriterTyped.h"
#include <nlohmann/json.hpp>

namespace WriterModule {
namespace hs00 {

using nlohmann::json;

void hs00_Writer::config_post_processing() {
  Json = json::parse("{\"shape\":" + ShapeField.getValue() + "}");
  TheWriterUntyped = createFromDataType();
}

InitResult hs00_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized. Make sure "
                             "that you call parse_config() before.");
  }
  TheWriterUntyped->createHDFStructure(HDFGroup, ChunkSize);
  return WriterModule::InitResult::OK;
}

WriterModule::InitResult hs00_Writer::reopen(hdf5::node::Group &HDFGroup) {
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized. Make sure "
                             "that you call parse_config() before.");
  }
  TheWriterUntyped = reOpenFromDataType(HDFGroup);
  if (!TheWriterUntyped) {
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

void hs00_Writer::write(FlatbufferMessage const &Message) {
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized. Make sure "
                             "that you call parse_config() before.");
  }
  TheWriterUntyped->write(Message);
}

WriterModule::ptr hs00_Writer::create() {
  return std::make_unique<hs00_Writer>();
}

WriterModule::Registry::Registrar<hs00_Writer> Register("hs00", "hs00");

WriterUntyped::ptr hs00_Writer::createFromDataType() {
  std::map<std::string, std::function<WriterUntyped::ptr()>> DataTypeMap{
      {"uint32", [&]() { return createFromEdgeType<uint32_t>(); }},
      {"uint64", [&]() { return createFromEdgeType<uint64_t>(); }},
      {"float", [&]() { return createFromEdgeType<float>(); }},
      {"double", [&]() { return createFromEdgeType<double>(); }}};
  try {
    return DataTypeMap.at(DataTypeField)();
  } catch (std::out_of_range const &) {
    throw std::runtime_error(
        fmt::format("unimplemented data_type: {:s}", DataTypeField.getValue()));
  }
}

template <typename DataType>
WriterUntyped::ptr hs00_Writer::createFromEdgeType() {
  std::map<std::string, std::function<WriterUntyped::ptr()>> EdgeTypeMap{
      {"uint32", [&]() { return createFromErrorType<DataType, uint32_t>(); }},
      {"uint64", [&]() { return createFromErrorType<DataType, uint64_t>(); }},
      {"float", [&]() { return createFromErrorType<DataType, float>(); }},
      {"double", [&]() { return createFromErrorType<DataType, double>(); }},
  };
  try {
    return EdgeTypeMap.at(EdgeTypeField)();
  } catch (std::out_of_range const &) {
    throw std::runtime_error(
        fmt::format("unimplemented edge_type: {:s}", EdgeTypeField.getValue()));
  }
}

template <typename DataType, typename EdgeType>
WriterUntyped::ptr hs00_Writer::createFromErrorType() {
  std::map<std::string, std::function<WriterUntyped::ptr()>> ErrorTypeMap{
      {"uint32_t",
       [&]() {
         return WriterTyped<DataType, EdgeType, uint32_t>::create(Json);
       }},
      {"uint64_t",
       [&]() {
         return WriterTyped<DataType, EdgeType, uint64_t>::create(Json);
       }},
      {"float",
       [&]() { return WriterTyped<DataType, EdgeType, float>::create(Json); }},
      {"double",
       [&]() { return WriterTyped<DataType, EdgeType, double>::create(Json); }},
  };
  try {
    return ErrorTypeMap.at(ErrorTypeField)();
  } catch (std::out_of_range const &) {
    throw std::runtime_error(fmt::format("unimplemented error_type: {:s}",
                                         ErrorTypeField.getValue()));
  }
}

/// Create the Writer during HDF reopen
WriterUntyped::ptr hs00_Writer::reOpenFromDataType(hdf5::node::Group &Group) {
  std::map<std::string, std::function<WriterUntyped::ptr()>> DataTypeMap{
      {"uint32", [&]() { return reOpenFromEdgeType<uint32_t>(Group); }},
      {"uint64", [&]() { return reOpenFromEdgeType<uint64_t>(Group); }},
      {"float", [&]() { return reOpenFromEdgeType<float>(Group); }},
      {"double", [&]() { return reOpenFromEdgeType<double>(Group); }},
  };
  try {
    return DataTypeMap.at(DataTypeField)();
  } catch (std::out_of_range const &) {
    throw std::runtime_error(
        fmt::format("unimplemented data_type: {:s}", DataTypeField.getValue()));
  }
}

template <typename DataType>
WriterUntyped::ptr hs00_Writer::reOpenFromEdgeType(hdf5::node::Group &Group) {
  std::map<std::string, std::function<WriterUntyped::ptr()>> DataTypeMap{
      {"uint32",
       [&]() { return reOpenFromErrorType<DataType, uint32_t>(Group); }},
      {"uint64",
       [&]() { return reOpenFromErrorType<DataType, uint64_t>(Group); }},
      {"float", [&]() { return reOpenFromErrorType<DataType, float>(Group); }},
      {"double",
       [&]() { return reOpenFromErrorType<DataType, double>(Group); }},
  };
  try {
    return DataTypeMap.at(EdgeTypeField)();
  } catch (std::out_of_range const &) {
    throw std::runtime_error(
        fmt::format("unimplemented edge_type: {:s}", EdgeTypeField.getValue()));
  }
}

template <typename DataType, typename EdgeType>
WriterUntyped::ptr hs00_Writer::reOpenFromErrorType(hdf5::node::Group &Group) {
  std::map<std::string, std::function<WriterUntyped::ptr()>> DataTypeMap{
      {"uint32",
       [&]() {
         return WriterTyped<DataType, EdgeType, uint32_t>::reOpen(Group);
       }},
      {"uint64",
       [&]() {
         return WriterTyped<DataType, EdgeType, uint64_t>::reOpen(Group);
       }},
      {"float",
       [&]() { return WriterTyped<DataType, EdgeType, float>::reOpen(Group); }},
      {"double",
       [&]() {
         return WriterTyped<DataType, EdgeType, double>::reOpen(Group);
       }},
  };
  try {
    return DataTypeMap.at(ErrorTypeField)();
  } catch (std::out_of_range const &) {
    throw std::runtime_error(fmt::format("unimplemented error_type: {:s}",
                                         ErrorTypeField.getValue()));
  }
}

} // namespace hs00
} // namespace WriterModule
