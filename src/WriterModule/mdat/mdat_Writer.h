// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "ModuleHDFInfo.h"
#include "TimeUtility.h"
#include "json.h"
#include "MultiVector.h"
#include "FileWriterTask.h"
#include "HDFOperations.h"

namespace WriterModule {
namespace mdat {

class mdat_Writer {
public:
  void declareWriteables(std::vector<ModuleHDFInfo> const& Modules) {
    Writables = extractDetails(Modules);
  }

  void setStartTime(time_point startTime) {
    StringValues["start_time"] = toUTCDateTime(startTime);
  }

  void setStopTime(time_point startTime) {
    StringValues["end_time"] = toUTCDateTime(startTime);
  }

  void writeMetadata(FileWriter::FileWriterTask const *Task) {
    for (auto const & Allowed : AllowedNames) {
      if (isWritable(Allowed)) {
        writeStringValue(Task, Writables[Allowed], Allowed, StringValues[Allowed]);
      }
    }
  };

private:
  bool isWritable(std::string const& Name) const {
    return Writables.find(Name) != Writables.end() && StringValues.find(Name) != StringValues.end();
  }

  void writeStringValue(FileWriter::FileWriterTask const *Task, std::string const &Path, std::string const &Name, std::string const& Value) {
    try {
      auto StringVec = MultiVector<std::string>{{1}};
      StringVec.at({0}) = Value;
      auto Group = hdf5::node::get_group(Task->hdfGroup(), Path);
      HDFOperations::writeStringDataset(Group, Name, StringVec);
    } catch (std::exception &Error) {
      LOG_ERROR("Failed to write time-point as ISO8601: {}", Error.what());
    }
  }

private:
  [[nodiscard]] std::unordered_map<std::string, std::string>
  extractDetails(std::vector<ModuleHDFInfo> const &Modules) const {
    std::unordered_map<std::string, std::string> Details;

    std::for_each(
        Modules.cbegin(), Modules.cend(), [&Details, this](auto const &Module) {
          if (Module.WriterModule == "mdat") {
            std::string name;
            nlohmann::json json = nlohmann::json::parse(Module.ConfigStream);
            for (auto it = json.begin(); it != json.end(); ++it) {
              if (it.key() == "name") {
                name = it.value();
              }
            }
            if (!name.empty() &&
                std::find(AllowedNames.begin(), AllowedNames.end(), name) !=
                    AllowedNames.end()) {
              Details[name] = Module.HDFParentName;
            }
          }
        });
    return Details;
  }

  std::vector<std::string> const AllowedNames{"start_time", "end_time"};
  std::unordered_map<std::string, std::string> Writables;
  std::unordered_map<std::string, std::string> StringValues;
};
} // namespace mdat
} // namespace WriterModule
