// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "ModuleSettings.h"
#include "MetaData/Tracker.h"
#include "Source.h"
#include "json.h"
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace FileWriter {

/// JSON parsing exception.
class ParseError : public std::runtime_error {
public:
  explicit ParseError(std::string const &err)
      : std::runtime_error("Parse ERROR: " + err) {}
};

/// \brief Represents the task of writing a HDF file.
///
/// It contains the list of `Source` and `DemuxTopic`
/// and makes those available to the FileMaster and `Streamer`.
/// Created by `Master` on command message and passed to FileMaster in vector.
class FileWriterTask {
public:
  /// Constructor
  ///
  /// \param TaskID The service ID.
  explicit FileWriterTask(MetaData::TrackerPtr const &Tracker)
      : MetaDataTracker(Tracker), FileSizeMB("", "approx_file_size_mb") {
    MetaDataTracker->registerMetaData(FileSizeMB);
  }

  ~FileWriterTask() = default;

  /// Initialise the HDF file.
  ///
  /// \param NexusStructure The structure of the NeXus file.
  /// \param HdfInfo The HDF information for the stream.
  /// \param UseSwmr Whether to use SWMR.
  void InitialiseHdf(std::string const &NexusStructure,
                     std::vector<ModuleHDFInfo> &HdfInfo);

  /// \brief  Set the `JobID`.
  ///
  /// \param Id The Id value to use.
  void setJobId(const std::string &Id);

  /// \brief Add a source to the demuxers.
  ///
  /// \param Source The source to add.
  void addSource(Source &&Source);

  /// \brief Set the filename.
  ///
  /// \param Prefix The path prefix.
  /// \param Name The filename (can include path).
  void setFilename(std::string const &Prefix, std::string const &Name);

  /// \brief Get the list of demuxers.
  ///
  /// \return The demux topics.
  std::vector<Source> &sources();

  /// \brief  Get the job ID of the file being written.
  ///
  /// \return The job ID.
  std::string jobID() const;

  /// \brief  Get the name of the file being written.
  ///
  /// \return The file name.
  std::string filename() const;

  /// Get the group for the HDF file.
  ///
  /// \return The group.
  hdf5::node::Group hdfGroup() const;

  void switchToWriteMode();

  bool isInWriteMode();
  
  void writeLinks(std::vector<ModuleSettings> const &LinkSettingsList);
  
  void writeMetaData();

  void flushDataToFile();

  /// \brief Updates the "arpproximate file size" meta data status field.
  /// \note Due to uncertainties in the file size, this function will round up
  /// to the closest 10MB.
  void updateApproximateFileSize();

private:
  std::string Filename;
  MetaData::TrackerPtr MetaDataTracker;
  MetaData::Value<uint32_t> FileSizeMB;

  /// \brief The HDF5 file object
  /// \note Must be located before the "source to module map" to guarantee that
  /// its destructor is not called before the writer modules have been
  /// de-allocated.
  std::unique_ptr<HDFFile> File;
  std::vector<Source> SourceToModuleMap;
  std::string JobId;
};

} // namespace FileWriter
