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
#include "HDFWriterModule.h"
#include "ProcessMessageResult.h"
#include "logger.h"
#include <string>

namespace FileWriter {

/// \brief Represents a sourcename on a topic.
///
/// The sourcename can be empty. This is meant for highest efficiency on topics
/// which are exclusively used for only one sourcename.
class Source final {
public:
  Source(std::string Name, std::string ID, HDFWriterModule::ptr Writer);
  Source(Source &&) = default;
  ~Source() = default;
  std::string const &topic() const;
  std::string const &sourcename() const;
  FlatbufferMessage::SrcHash getHash() const { return Hash; };
  ProcessMessageResult process_message(FlatbufferMessage const &Message);
  HDFFile *HDFFileForSWMR = nullptr;
  void setTopic(std::string const &Name);

private:
  std::string TopicName;
  std::string SourceName;
  std::string SchemaID;
  FlatbufferMessage::SrcHash Hash;
  std::unique_ptr<HDFWriterModule> WriterModule;
  SharedLogger Logger = getLogger();
};

} // namespace FileWriter
