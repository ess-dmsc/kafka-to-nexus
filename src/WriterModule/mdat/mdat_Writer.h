// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "FileWriterTask.h"
#include "ModuleHDFInfo.h"
#include "TimeUtility.h"

namespace WriterModule::mdat {

/// \brief Used to write basic metadata such as start time, etc.
///
/// It works differently to other writer modules in that it doesn't listen to
/// a Kafka topic. Instead values to be written are set in regular code.
class mdat_Writer {
public:
  /// \brief Work out what data to write based on the contents of mdat modules.
  ///
  /// \param Modules
  void define_metadata(std::vector<ModuleHDFInfo> const &modules);

  /// \brief Set start time which should be written to the file.
  ///
  /// \param startTime
  void set_start_time(time_point start_time);

  /// \brief Set stop time which should be written to the file.
  ///
  /// \param startTime
  void set_stop_time(time_point stop_time);

  /// \brief Write any defined values to the HDF file.
  ///
  /// \note Nothing will be written until this is called.
  ///
  /// \param Task
  void write_metadata(FileWriter::FileWriterTask const &task) const;

private:
  struct Writable {
    std::string path;
    std::string value;

    [[nodiscard]] bool isWritable() const {
      return !path.empty() && !value.empty();
    }
  };

  void static write_string_value(FileWriter::FileWriterTask const &task,
                                 std::string const &name,
                                 std::string const &path,
                                 std::string const &value);

  [[nodiscard]] std::unordered_map<std::string, Writable>
  extract_details(std::vector<ModuleHDFInfo> const &modules) const;

  [[nodiscard]] std::vector<std::string> static extract_names(
      std::string const &config_json);

  void set_writable_value_if_defined(std::string const &name,
                                     time_point const &time);

  inline static const std::string _start_time = "start_time";
  inline static const std::string _end_time = "end_time";
  std::vector<std::string> const _allowed_names{_start_time, _end_time};
  std::unordered_map<std::string, Writable> _writables;
};
} // namespace WriterModule::mdat
