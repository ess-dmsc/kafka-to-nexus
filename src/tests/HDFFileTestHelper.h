#pragma once

#include "../HDFFile.h"
#include "../MainOpt.h"
#include <h5cpp/hdf5.hpp>

void SetTestOptions(MainOpt *Options);

namespace HDFFileTestHelper {

FileWriter::HDFFile createInMemoryTestFile(const std::string &Filename);

template <typename T>
std::string
createCommandForDataset(const std::pair<std::string, T> NameAndValue) {
  using namespace fmt::literals;

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
