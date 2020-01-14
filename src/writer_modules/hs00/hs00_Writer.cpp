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

namespace hs00 {

void hs00_Writer::parse_config(std::string const &ConfigurationStream) {
  TheWriterUntyped = WriterUntyped::createFromJson(
      WriterUntyped::json::parse(ConfigurationStream));
}

FileWriter::HDFWriterModule::InitResult
hs00_Writer::init_hdf(hdf5::node::Group &HDFGroup, std::string const &) {
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized. Make sure "
                             "that you call parse_config() before.");
  }
  TheWriterUntyped->createHDFStructure(HDFGroup, ChunkBytes);
  return FileWriter::HDFWriterModule::InitResult::OK;
}

FileWriter::HDFWriterModule::InitResult
hs00_Writer::reopen(hdf5::node::Group &HDFGroup) {
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

void hs00_Writer::write(FlatbufferMessage const &Message) {
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized. Make sure "
                             "that you call parse_config() before.");
  }
  TheWriterUntyped->write(Message, DoFlushEachWrite);
}

FileWriter::HDFWriterModule::ptr hs00_Writer::create() {
  return FileWriter::HDFWriterModule::ptr(new hs00_Writer);
}

FileWriter::HDFWriterModuleRegistry::Registrar<hs00_Writer> RegisterWriter("hs00");
} // namespace hs00
