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
#include "HDFOperations.h"
#include "ModuleHDFInfo.h"
#include "MultiVector.h"
#include "TimeUtility.h"
#include "json.h"

namespace WriterModule::mdat {
void mdat_Writer::define_metadata(std::vector<ModuleHDFInfo> const &modules) {
  _writables = extract_details(modules);
}

void mdat_Writer::set_start_time(time_point start_time) {
  set_writable_value_if_defined(_start_time, start_time);
}

void mdat_Writer::set_stop_time(time_point stop_time) {
  set_writable_value_if_defined(_end_time, stop_time);
}

void mdat_Writer::set_writable_value_if_defined(const std::string &name,
                                                const time_point &time) {
  if (auto result = _writables.find(name); result != _writables.end()) {
    result->second.value = toUTCDateTime(time);
  }
}

void mdat_Writer::write_metadata(FileWriter::FileWriterTask const &task) const {
  for (auto const &[name, value] : _writables) {
    if (std::find(_allowed_names.cbegin(), _allowed_names.cend(), name) ==
        _allowed_names.end()) {
      continue;
    }
    if (value.isWritable()) {
      write_string_value(task, name, value.path, value.value);
    }
  }
}

void mdat_Writer::write_string_value(FileWriter::FileWriterTask const &task,
                                     std::string const &name,
                                     std::string const &path,
                                     std::string const &value) {
  try {
    auto string_vec = MultiVector<std::string>{{1}};
    string_vec.set_value({0}, value);
    auto group = hdf5::node::get_group(task.hdfGroup(), path);
    HDFOperations::writeStringDataset(group, name, string_vec);
  } catch (std::exception &error) {
    LOG_ERROR("Failed to write mdat string value: {}", error.what());
  }
}

std::unordered_map<std::string, mdat_Writer::Writable>
mdat_Writer::extract_details(const std::vector<ModuleHDFInfo> &modules) const {
  std::unordered_map<std::string, Writable> details;

  for (auto const &module : modules) {
    if (module.WriterModule != "mdat") {
      continue;
    }

    auto const names = extract_names(module.ConfigStream);
    for (auto const &name : names) {
      if (std::find(_allowed_names.begin(), _allowed_names.end(), name) !=
          _allowed_names.end()) {
        Writable new_writable;
        new_writable.path = module.HDFParentName;
        details[name] = new_writable;
      }
    }
  }
  return details;
}

std::vector<std::string>
mdat_Writer::extract_names(const std::string &config_json) {
  nlohmann::json json = nlohmann::json::parse(config_json);
  for (auto it = json.begin(); it != json.end(); ++it) {
    if (it.key() == "items" && !it.value().empty()) {
      return it->get<std::vector<std::string>>();
    }
  }
  return {};
}
} // namespace WriterModule::mdat
