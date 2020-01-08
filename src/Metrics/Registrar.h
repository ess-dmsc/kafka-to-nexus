#pragma once

#include "Metric.h"
#include "MetricsList.h"
#include "Sink.h"
#include <algorithm>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace Metrics {

/// Register and deregister metrics to be reported via a specified sink
/// threadsafe
class Registrar {
public:
  void registerMetric(Metric &NewMetric, std::vector<LogTo> const &SinkTypes) {
    std::lock_guard<std::mutex> Lock(MetricListsMutex);
    for (auto &SinkTypeAndMetric : MetricLists) {
      if (std::find(SinkTypes.begin(), SinkTypes.end(),
                    SinkTypeAndMetric.first) != SinkTypes.end()) {
        SinkTypeAndMetric.second->addMetric(NewMetric);
      }
    }
  };

  void deregisterMetric(std::string const &MetricName) {
    std::lock_guard<std::mutex> Lock(MetricListsMutex);
    for (auto &SinkTypeAndMetric : MetricLists) {
      SinkTypeAndMetric.second->tryRemoveMetric(MetricName);
    }
  };

  void addMetricsList(LogTo SinkType,
                      std::shared_ptr<MetricsList> const &NewMetricsList) {
    std::lock_guard<std::mutex> Lock(MetricListsMutex);
    MetricLists.emplace(SinkType, NewMetricsList);
  };

private:
  std::mutex MetricListsMutex;
  /// Ownership of each MetricsList is shared with the Reporter which is
  /// responsible for it
  std::map<LogTo, std::shared_ptr<MetricsList>> MetricLists;
};

} // namespace Metrics
