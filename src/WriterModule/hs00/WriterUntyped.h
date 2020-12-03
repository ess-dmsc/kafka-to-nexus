// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Shape.h"
#include "WriterModuleBase.h"
#include <h5cpp/hdf5.hpp>
#include <vector>

namespace WriterModule {
namespace hs00 {

using FileWriter::FlatbufferMessage;

class WriterUntyped {
public:
  using ptr = std::unique_ptr<WriterUntyped>;
  /// Create a WriterTyped from Json.

  /// \brief Create the HDF structures.
  ///
  /// Used on arrival of a new write command for the file writer to create the
  /// initial structure of the HDF file for this writer module.
  virtual void createHDFStructure(hdf5::node::Group &Group,
                                  size_t ChunkSize) = 0;

  virtual void write(FlatbufferMessage const &Message) = 0;

  virtual ~WriterUntyped() = default;
};
} // namespace hs00
} // namespace WriterModule
