// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Tracker.h"

namespace Statistics {

void Tracker::registerStatistic(Statistics::ValueBase NewStatistic) {
  std::lock_guard LockGuard(StatisticsMutex);
  if (NewStatistic.getValuePtr() != nullptr) {
    KnownStatistics.emplace_back(NewStatistic.getValuePtr());
  }
}
void Tracker::clearStatistics() {
  std::lock_guard LockGuard(StatisticsMutex);
  KnownStatistics.clear();
}

void Tracker::writeToJSONDict(nlohmann::json &JSONNode) const {
  std::lock_guard LockGuard(StatisticsMutex);
  for (auto const &Statistics : KnownStatistics) {
    auto JSONObj = Statistics->getAsJSON();
    JSONNode.insert(JSONObj.cbegin(), JSONObj.cend());
  }
}

void Tracker::writeToHDF5File(hdf5::node::Group RootNode) const {
  std::lock_guard LockGuard(StatisticsMutex);
  int ErrorCounter{0};
  for (auto const &Statistics : KnownStatistics) {
    try {
      Statistics->writeToHDF5File(RootNode);
    } catch (std::exception const &E) {
      ErrorCounter++;
    }
  }
  if (ErrorCounter > 0) {
    LOG_ERROR(
        "Failed to write {} (out of a total of {}) meta-data values to file.",
        ErrorCounter, KnownStatistics.size());
  }
}

} // namespace Statistics