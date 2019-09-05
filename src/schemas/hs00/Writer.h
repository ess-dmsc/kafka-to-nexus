// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include "WriterUntyped.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename T> using uptr = std::unique_ptr<T>;

class Writer : public FileWriter::HDFWriterModule {
public:
  static FileWriter::HDFWriterModule::ptr create();
  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override;
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void write(FlatbufferMessage const &Message) override;
  int32_t flush() override;
  int32_t close() override;

  WriterUntyped::ptr TheWriterUntyped;

  hsize_t ChunkBytes = 1 << 21;
  bool DoFlushEachWrite = true;
  uint64_t TotalWrittenBytes = 0;

private:
  SharedLogger Logger = spdlog::get("filewriterlogger");
};
} // namespace hs00
} // namespace Schemas
} // namespace FileWriter
