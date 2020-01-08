// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Metric.h"
#include "logger.h"
#include <map>
#include <mutex>

namespace Metrics {

/// Holds the Metrics to be reported on by a particular Reporter
/// Owned by the Registrar and a Reporter
class MetricsList {
public:
  void addMetric(Metric &NewMetric) {
    std::lock_guard<std::mutex> Lock(ListMutex);
    if (ListOfMetrics.find(NewMetric.getName()) != ListOfMetrics.end()) {
      Logger->error(
          "Metric name is not unique, cannot add to list to be reported");
      return;
    }
    ListOfMetrics.emplace(NewMetric.getName(), NewMetric);
  };

  /// Removes Metric by name if it exists in this list
  void tryRemoveMetric(std::string const &MetricName) {
    ListOfMetrics.erase(MetricName);
  };

private:
  std::map<std::string, Metric &> ListOfMetrics; // MetricName: Metric
  std::mutex ListMutex;
  SharedLogger Logger = getLogger();
};

} // namespace Metrics
