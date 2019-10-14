// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "StreamMaster.h"
#include <memory>
#include <vector>

namespace FileWriter {
class StreamsController {
public:
  /// \brief Add a new job.
  ///
  /// \param StreamMaster The job to add.
  void addStreamMaster(std::unique_ptr<IStreamMaster> StreamMaster);

  /// \brief Stop all the jobs.
  void stopStreamMasters();

  /// \brief Check with a job ID is already being used.
  ///
  /// \param JobID The ID to check for.
  /// \return true if in use.
  bool jobIDInUse(std::string const &JobID);

  /// \brief Stop a job based on an ID.
  ///
  /// \param JobID The ID of the job to stop.
  void stopJob(std::string const &JobID);

  /// \brief Set the stop time for a specific job.
  ///
  /// \param JobID The job.
  /// \param StopTime The stop time.
  void setStopTimeForJob(std::string const &JobID,
                         std::chrono::milliseconds const &StopTime);

  /// \brief Retrieve the associated job.
  ///
  /// \param JobID The ID.
  /// \return The associated job.
  std::unique_ptr<IStreamMaster> &
  getStreamMasterForJobID(std::string const &JobID);

  /// \brief Delete any jobs that have been marked removable.
  void deleteRemovable();

  /// \brief Publish the stats for all the jobs to Kafka
  ///
  /// \param Producer The producer to publish via.
  /// \param ServiceID The service ID.
  void
  publishStreamStats(std::shared_ptr<KafkaW::ProducerTopic> const &Producer,
                     std::string const &ServiceID);

private:
  std::vector<std::unique_ptr<IStreamMaster>> StreamMasters;
};
} // namespace FileWriter
