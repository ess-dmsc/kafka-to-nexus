// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Source.h"
#include "json.h"
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace KafkaW {
class ProducerTopic;
};

namespace FileWriter {
class DemuxTopic;

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
class FileWriterTask final {
public:
  /// Constructor
  ///
  /// \param TaskID The service ID.
  FileWriterTask(std::string TaskID)
      : ServiceId(std::move(TaskID)), Logger(getLogger()){};

  /// Destructor.
  ~FileWriterTask();

  /// Initialise the HDF file.
  ///
  /// \param NexusStructure The structure of the NeXus file.
  /// \param ConfigFile The configuration information.
  /// \param HdfInfo The HDF information for the stream.
  /// \param UseSwmr Whether to use SWMR.
  void InitialiseHdf(std::string const &NexusStructure,
                     std::string const &ConfigFile,
                     std::vector<StreamHDFInfo> &HdfInfo, bool UseSwmr);

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
  std::map<std::string, std::shared_ptr<DemuxTopic>> &demuxers();

  /// \brief  Get the job ID of the file being written.
  ///
  /// \return The job ID.
  std::string jobID() const;

  /// \brief  Name of the file being written.
  ///
  /// Important for reopening of files.
  ///
  /// \return The file name.
  std::string filename() const;

  /// Get the group for the HDF file.
  ///
  /// \return The group.
  hdf5::node::Group hdfGroup();

  /// Get whether SWMR is enabled for this task.
  ///
  /// \return true if enabled.
  bool swmrEnabled() const;

private:
  std::string Filename;
  std::map<std::string, std::shared_ptr<DemuxTopic>> TopicNameToDemuxerMap;
  void closeFile();
  void reopenFile();
  std::string JobId;
  std::string ServiceId;
  HDFFile File;
  SharedLogger Logger;
};

} // namespace FileWriter
