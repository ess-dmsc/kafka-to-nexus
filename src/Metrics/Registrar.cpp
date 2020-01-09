#include "Registrar.h"
#include "Metric.h"
#include <algorithm>

namespace Metrics {

void Registrar::registerMetric(Metric &NewMetric,
                               std::vector<LogTo> const &SinkTypes) {
  std::lock_guard<std::mutex> Lock(MetricListsMutex);
  for (auto &SinkTypeAndMetric : MetricLists) {
    if (std::find(SinkTypes.begin(), SinkTypes.end(),
                  SinkTypeAndMetric.first) != SinkTypes.end()) {
      SinkTypeAndMetric.second->addMetric(NewMetric);
    }
  }
};

void Registrar::deregisterMetric(std::string const &MetricName) {
  std::lock_guard<std::mutex> Lock(MetricListsMutex);
  for (auto &SinkTypeAndMetric : MetricLists) {
    SinkTypeAndMetric.second->tryRemoveMetric(MetricName);
  }
};

void Registrar::addMetricsList(
    LogTo SinkType, std::shared_ptr<MetricsList> const &NewMetricsList) {
  std::lock_guard<std::mutex> Lock(MetricListsMutex);
  MetricLists.emplace(SinkType, NewMetricsList);
};

} // namespace Metrics
