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
#include "HDF5/HDFFile.h"
#include "WriterModuleBase.h"
#include "logger.h"
#include <string>

namespace FileWriter {

/// \brief Represents a sourcename on a topic.
class Source {
public:
  Source(std::string Name, std::string FlatbufferID, std::string ModuleID,
         std::string Topic, WriterModule::ptr Writer);
  Source(Source &&) = default;
  ~Source() = default;
  std::string const &topic() const;
  std::string const &sourcename() const;
  std::string const &flatbufferID() const { return SchemaID; };
  std::string const &writerModuleID() const { return WriterModuleID; };
  FlatbufferMessage::SrcHash getSrcHash() const { return SrcHash; };
  FlatbufferMessage::SrcHash getModuleHash() const { return ModuleHash; };
  WriterModule::Base *getWriterPtr() { return WriterModule.get(); }

private:
  std::string SourceName;
  std::string SchemaID;
  std::string WriterModuleID;
  std::string TopicName;
  FlatbufferMessage::SrcHash SrcHash;
  FlatbufferMessage::SrcHash ModuleHash;
  std::unique_ptr<WriterModule::Base> WriterModule;
};

} // namespace FileWriter
