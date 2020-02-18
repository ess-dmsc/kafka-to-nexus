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

Source::Source(std::string Name, std::string ID, std::string Topic,
               WriterModule::ptr Writer)
    : SourceName(std::move(Name)), SchemaID(std::move(ID)),
      TopicName(std::move(Topic)), Hash(calcSourceHash(SchemaID, SourceName)),
      WriterModule(std::move(Writer)) {}

std::string const &Source::topic() const { return TopicName; }

std::string const &Source::sourcename() const { return SourceName; }

} // namespace FileWriter
