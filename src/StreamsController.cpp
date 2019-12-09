// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file This file defines the different success and failure status that the
/// `StreamMaster` and the `Streamer` can incur. These error object have some
/// utility methods that can be used to test the more common situations.

#include "StreamsController.h"

namespace FileWriter {
void StreamsController::addStreamMaster(
    std::unique_ptr<IStreamMaster> StreamMaster) {
  StreamMasters.emplace_back(std::move(StreamMaster));
}

void StreamsController::stopStreamMasters() {
  for (auto &SM : StreamMasters) {
    SM->requestStop();
  }
}

bool StreamsController::isRunning() {
  return not StreamMasters.empty();
}

void StreamsController::stopJob(std::string const &JobID) {
  getStreamMasterForJobID(JobID)->requestStop();
}

void StreamsController::setStopTimeForJob(
    std::string const &JobID, std::chrono::milliseconds const &StopTime) {
  getStreamMasterForJobID(JobID)->setStopTime(StopTime);
}

std::unique_ptr<IStreamMaster> &
StreamsController::getStreamMasterForJobID(std::string const &JobID) {
  for (auto &StreamMaster : StreamMasters) {
    if (StreamMaster->getJobId() == JobID) {
      return StreamMaster;
    }
  }
  throw std::runtime_error(
      fmt::format("Could not find stream master with job id {}", JobID));
}

void StreamsController::deleteRemovable() {
  StreamMasters.erase(std::remove_if(StreamMasters.begin(), StreamMasters.end(),
                                     [](std::unique_ptr<IStreamMaster> &Iter) {
                                       return Iter->isRemovable();
                                     }),
                      StreamMasters.end());
}

void StreamsController::publishStreamStats(
    std::shared_ptr<KafkaW::ProducerTopic> const &Producer,
    std::string const &ServiceID) {
  auto Status = nlohmann::json::object();
  Status["type"] = "filewriter_status_master";
  Status["service_id"] = ServiceID;
  Status["files"] = nlohmann::json::object();

  for (auto &StreamMaster : StreamMasters) {
    auto FilewriterTaskID = fmt::format("{}", StreamMaster->getJobId());
    auto FilewriterTaskStatus = StreamMaster->getStats();
    Status["files"][FilewriterTaskID] = FilewriterTaskStatus;
  }
  Producer->produce(Status.dump());
}
}
