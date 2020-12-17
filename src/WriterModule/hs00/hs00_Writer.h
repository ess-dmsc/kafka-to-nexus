// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "FlatbufferReader.h"
#include "WriterModuleBase.h"
#include "WriterUntyped.h"

namespace WriterModule {
namespace hs00 {

template <typename T> using uptr = std::unique_ptr<T>;

class hs00_Writer : public WriterModule::Base {
public:
  hs00_Writer() : WriterModule::Base(false, "NXdata") {}
  static WriterModule::ptr create();
  void config_post_processing() override;
  InitResult init_hdf(hdf5::node::Group &HDFGroup) override;
  InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void write(FlatbufferMessage const &Message) override;

  WriterUntyped::ptr TheWriterUntyped;

private:
  SharedLogger Logger = spdlog::get("filewriterlogger");
  JsonConfig::Field<size_t> ChunkSize{this, "chunk_size", 1 << 20};
  JsonConfig::RequiredField<std::string> DataTypeField{this,
                                                               "data_type"};
  JsonConfig::RequiredField<std::string> EdgeTypeField{this,
                                                               "edge_type"};
  JsonConfig::RequiredField<std::string> ErrorTypeField{this,
                                                                "error_type"};
  JsonConfig::RequiredField<std::string> ShapeField{this, "shape"};
  nlohmann::json Json;

  /// Create the Writer during HDF reopen
  WriterUntyped::ptr reOpenFromDataType(hdf5::node::Group &Group);

  WriterUntyped::ptr createFromDataType();

  template <typename DataType> WriterUntyped::ptr createFromEdgeType();

  template <typename DataType, typename EdgeType>
  WriterUntyped::ptr createFromErrorType();

  template <typename DataType>
  WriterUntyped::ptr reOpenFromEdgeType(hdf5::node::Group &Group);

  template <typename DataType, typename EdgeType>
  WriterUntyped::ptr reOpenFromErrorType(hdf5::node::Group &Group);
};
} // namespace hs00
} // namespace WriterModule
