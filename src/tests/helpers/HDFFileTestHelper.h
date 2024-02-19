// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "HDF5/HDFFile.h"
#include "MainOpt.h"
#include <h5cpp/hdf5.hpp>

namespace HDFFileTestHelper {

class DebugHDFFile : public FileWriter::HDFFileBase {
public:
  using FileWriter::HDFFileBase::init;
};

class InMemoryHDFFile : public DebugHDFFile {
public:
  InMemoryHDFFile();
};

class DiskHDFFile : public DebugHDFFile {
public:
  explicit DiskHDFFile(std::string const &FileName);
};

std::unique_ptr<DebugHDFFile>
createInMemoryTestFile(const std::string &Filename, bool OnDisk = false);

template <typename T>
std::string
createCommandForDataset(const std::pair<std::string, T> NameAndValue) {
  using fmt::literals::operator""_a;

  std::string Command = R"(  <
    "name": "{name}",
    "values": {value},
    "type": "dataset",
    "dataset": <
      "type": "{type_string}"
    >
  >)";

  Command = fmt::format(Command, "name"_a = NameAndValue.first,
                        "value"_a = NameAndValue.second,
                        "type_string"_a = NameAndValue.first);
  std::replace(Command.begin(), Command.end(), '<', '{');
  std::replace(Command.begin(), Command.end(), '>', '}');

  return Command;
}
} // namespace HDFFileTestHelper
