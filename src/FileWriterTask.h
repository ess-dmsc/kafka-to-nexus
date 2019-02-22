#pragma once

#include "DemuxTopic.h"
#include "KafkaW/ProducerTopic.h"
#include "Source.h"
#include "json.h"
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
class FileWriterTask final {
public:
  /// Constructor
  ///
  /// \param TaskID The service ID.
  /// \param StatusProducer_ The status producer.
  FileWriterTask(std::string TaskID,
                 std::shared_ptr<KafkaW::ProducerTopic> StatusProducerPtr);

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
  std::vector<DemuxTopic> &demuxers();

  /// \brief  Get the unique numeric identifier of this job.
  ///
  /// Could maybe be replaced by `JobID`.
  ///
  /// \return The unique identifier.
  uint64_t id() const;

  /// \brief  Get the job ID of the file being written.
  ///
  /// \return The job ID.
  std::string jobID() const;

  /// \brief  Return statistics about this job as JSON.
  nlohmann::json stats() const;

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
  std::vector<DemuxTopic> Demuxers;
  void closeFile();
  void reopenFile();
  uint64_t Id;
  std::string JobId;
  std::string ServiceId;
  std::shared_ptr<KafkaW::ProducerTopic> StatusProducer;
  HDFFile File;
  std::shared_ptr<spdlog::logger> Logger;
};

} // namespace FileWriter
