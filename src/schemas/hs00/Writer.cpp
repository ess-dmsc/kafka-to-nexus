// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Writer.h"
#include "Exceptions.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

void Writer::parse_config(std::string const &ConfigurationStream) {
  TheWriterUntyped = WriterUntyped::createFromJson(
      WriterUntyped::json::parse(ConfigurationStream));
}

FileWriter::HDFWriterModule::InitResult
Writer::init_hdf(hdf5::node::Group &HDFGroup, std::string const &) {
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized. Make sure "
                             "that you call parse_config() before.");
  }
  TheWriterUntyped->createHDFStructure(HDFGroup, ChunkBytes);
  return FileWriter::HDFWriterModule::InitResult::OK;
}

FileWriter::HDFWriterModule::InitResult
Writer::reopen(hdf5::node::Group &HDFGroup) {
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized. Make sure "
                             "that you call parse_config() before.");
  }
  TheWriterUntyped = WriterUntyped::createFromHDF(HDFGroup);
  if (!TheWriterUntyped) {
    return FileWriter::HDFWriterModule::InitResult::ERROR;
  }
  return FileWriter::HDFWriterModule::InitResult::OK;
}

void Writer::write(FlatbufferMessage const &Message) {
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized. Make sure "
                             "that you call parse_config() before.");
  }
  TheWriterUntyped->write(Message, DoFlushEachWrite);
}

FileWriter::HDFWriterModule::ptr Writer::create() {
  return FileWriter::HDFWriterModule::ptr(new Writer);
}

HDFWriterModuleRegistry::Registrar<Writer> RegisterWriter("hs00");
} // namespace hs00
} // namespace Schemas
} // namespace FileWriter
