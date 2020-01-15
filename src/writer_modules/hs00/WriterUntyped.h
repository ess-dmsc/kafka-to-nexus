// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "HDFWriterModule.h"
#include "Shape.h"
#include "json.h"
#include <h5cpp/hdf5.hpp>
#include <vector>

namespace Module {
namespace hs00 {

using FileWriter::FlatbufferMessage;

class WriterUntyped {
public:
  using ptr = std::unique_ptr<WriterUntyped>;
  using json = nlohmann::json;
  /// Create a WriterTyped from Json.
  static ptr createFromJson(json const &Json);

  /// Create the Writer during HDF reopen
  static ptr createFromHDF(hdf5::node::Group &Group);

  /// \brief Create the HDF structures.
  ///
  /// Used on arrival of a new write command for the file writer to create the
  /// initial structure of the HDF file for this writer module.
  virtual void createHDFStructure(hdf5::node::Group &Group,
                                  size_t ChunkBytes) = 0;

  virtual void write(FlatbufferMessage const &Message,
                     bool DoFlushEachWrite) = 0;

  virtual ~WriterUntyped() = default;

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
} // namespace hs00
} // namespace Module
