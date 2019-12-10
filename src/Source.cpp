// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Source.h"

namespace FileWriter {

Source::Source(std::string Name, std::string ID, HDFWriterModule::ptr Writer)
    : SourceName(std::move(Name)), SchemaID(std::move(ID)),
      Hash(calcSourceHash(SchemaID, SourceName)),
      WriterModule(std::move(Writer)) {}

std::string const &Source::topic() const { return TopicName; }

std::string const &Source::sourcename() const { return SourceName; }

ProcessMessageResult Source::process_message(FlatbufferMessage const &Message) {
  if (std::string(Message.data() + 4, Message.data() + 8) != SchemaID) {
    Logger->trace("SchemaID: {} not accepted by source_name: {}", SchemaID,
                  SourceName);
    return ProcessMessageResult::ERR;
  }

  try {
    WriterModule->write(Message);
    if (HDFFileForSWMR != nullptr) {
      HDFFileForSWMR->SWMRFlush();
    }
  } catch (const HDFWriterModuleRegistry::WriterException &E) {
    Logger->error("Failure while writing message: {}", E.what());
    return ProcessMessageResult::ERR;
  }

  return ProcessMessageResult::OK;
}

void Source::setTopic(std::string const &Name) { TopicName = Name; }

} // namespace FileWriter
