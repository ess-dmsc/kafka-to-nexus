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

#pragma once

#include "StreamMaster.h"
#include <memory>
#include <vector>

namespace FileWriter {
class StreamsController {
public:
  void addStreamMaster(std::unique_ptr<IStreamMaster> StreamMaster) {
    StreamMasters.emplace_back(std::move(StreamMaster));
  }

  void stopStreamMasters() {
    for (auto &SM : StreamMasters) {
      SM->requestStop();
    }
  }

  std::unique_ptr<IStreamMaster> &
  getStreamMasterForJobID(std::string const &JobID) {
    for (auto &StreamMaster : StreamMasters) {
      if (StreamMaster->getJobId() == JobID) {
        return StreamMaster;
      }
    }
    throw std::runtime_error(
        fmt::format("Could not find stream master with job id {}", JobID));
  }

  void deleteRemovable() {
    StreamMasters.erase(
        std::remove_if(StreamMasters.begin(), StreamMasters.end(),
                       [](std::unique_ptr<IStreamMaster> &Iter) {
                         return Iter->isRemovable();
                       }),
        StreamMasters.end());
  }

private:
  std::vector<std::unique_ptr<IStreamMaster>> StreamMasters;
};
} // namespace FileWriter
