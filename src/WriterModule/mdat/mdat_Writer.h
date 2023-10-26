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
  void defineMetadata(std::vector<ModuleHDFInfo> const &Modules);

  /// \brief Set start time which should be written to the file.
  ///
  /// \param startTime
  void setStartTime(time_point startTime);

  /// \brief Set stop time which should be written to the file.
  ///
  /// \param startTime
  void setStopTime(time_point startTime);

  /// \brief Write any defined values to the HDF file.
  ///
  /// \note Nothing will be written until this is called.
  ///
  /// \param Task
  void writeMetadata(FileWriter::FileWriterTask const *Task);

private:
  [[nodiscard]] bool isWritable(std::string const &Name) const;

  void static writeStringValue(FileWriter::FileWriterTask const *Task,
                               std::string const &Path, std::string const &Name,
                               std::string const &Value);

  [[nodiscard]] std::unordered_map<std::string, std::string>
  extractDetails(std::vector<ModuleHDFInfo> const &Modules) const;

  [[nodiscard]] std::optional<std::string> static extractName(
      std::string const &configJson);

  std::vector<std::string> const AllowedNames{"start_time", "end_time"};
  std::unordered_map<std::string, std::string> Writables;
  std::unordered_map<std::string, std::string> StringValues;
};
} // namespace WriterModule::mdat
