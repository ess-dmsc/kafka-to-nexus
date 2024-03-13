// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "mdat_Writer.h"
#include "FileWriterTask.h"
#include "HDF5/HDFOperations.h"
#include "HDF5/MultiVector.h"
#include "ModuleHDFInfo.h"
#include "TimeUtility.h"
#include "json.h"

namespace WriterModule::mdat {
void mdat_Writer::defineMetadata(std::vector<ModuleHDFInfo> const &Modules) {
  Writables = extractDetails(Modules);
}

void mdat_Writer::setStartTime(time_point startTime) {
  setWritableValueIfDefined(StartTime, startTime);
}

void mdat_Writer::setStopTime(time_point stopTime) {
  setWritableValueIfDefined(EndTime, stopTime);
}

void mdat_Writer::setWritableValueIfDefined(std::string const &Name,
                                            time_point const &Time) {
  if (auto Result = Writables.find(Name); Result != Writables.end()) {
    Result->second.Value = toUTCDateTime(Time);
  }
}

void mdat_Writer::writeMetadata(FileWriter::FileWriterTask const *Task) const {
  for (auto const &[Name, Value] : Writables) {
    if (std::find(AllowedNames.cbegin(), AllowedNames.cend(), Name) ==
        AllowedNames.end()) {
      continue;
    }
    if (Value.isWritable()) {
      writeStringValue(Task, Name, Value.Path, Value.Value);
    }
  }
}

void mdat_Writer::writeStringValue(FileWriter::FileWriterTask const *Task,
                                   std::string const &Name,
                                   std::string const &Path,
                                   std::string const &Value) {
  try {
    auto StringVec = MultiVector<std::string>{{1}};
    StringVec.at({0}) = Value;
    auto Group = hdf5::node::get_group(Task->hdfGroup(), Path);
    HDFOperations::writeStringDataset(Group, Name, StringVec);
  } catch (std::exception &Error) {
    LOG_ERROR("Failed to write mdat string value: {}", Error.what());
  }
}

std::unordered_map<std::string, mdat_Writer::Writable>
mdat_Writer::extractDetails(std::vector<ModuleHDFInfo> const &Modules) const {
  std::unordered_map<std::string, Writable> Details;

  for (auto const &Module : Modules) {
    if (Module.WriterModule != "mdat") {
      continue;
    }

    auto const name = extractName(Module.ConfigStream);
    if (name && std::find(AllowedNames.begin(), AllowedNames.end(), name) !=
                    AllowedNames.end()) {
      Writable NewWritable;
      NewWritable.Path = Module.HDFParentName;
      Details[name.value()] = NewWritable;
    }
  }
  return Details;
}

std::optional<std::string>
mdat_Writer::extractName(std::string const &configJson) {
  nlohmann::json json = nlohmann::json::parse(configJson);
  for (auto it = json.begin(); it != json.end(); ++it) {
    if (it.key() == "name" && !it.value().empty()) {
      return it.value();
    }
  }
  return {};
}
} // namespace WriterModule::mdat
