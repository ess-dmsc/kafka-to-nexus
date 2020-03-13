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
#include "HDFFile.h"
#include "WriterModuleBase.h"
#include "logger.h"
#include <string>

namespace FileWriter {

/// \brief Represents a sourcename on a topic.
///
/// The sourcename can be empty. This is meant for highest efficiency on topics
/// which are exclusively used for only one sourcename.
class Source final {
public:
  Source(std::string Name, std::string ID, std::string Topic,
         WriterModule::ptr Writer);
  Source(Source &&) = default;
  ~Source() = default;
  std::string const &topic() const;
  std::string const &sourcename() const;
  std::string const &flatbufferID() const { return SchemaID; };
  FlatbufferMessage::SrcHash getHash() const { return Hash; };
  WriterModule::Base *getWriterPtr() { return WriterModule.get(); }

private:
  std::string SourceName;
  std::string SchemaID;
  std::string TopicName;
  FlatbufferMessage::SrcHash Hash;
  std::unique_ptr<WriterModule::Base> WriterModule;
};

} // namespace FileWriter
