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
      std::string NewName = prependPrefix(NewMetric.getName());
      SinkTypeAndMetric.second->addMetric(NewMetric, NewName);
    }
  }
}

void Registrar::deregisterMetric(std::string const &MetricName) {
  std::lock_guard<std::mutex> Lock(MetricListsMutex);
  for (auto &SinkTypeAndMetric : MetricLists) {
    SinkTypeAndMetric.second->tryRemoveMetric(prependPrefix(MetricName));
  }
}

void Registrar::addMetricsList(
    LogTo SinkType, std::shared_ptr<MetricsList> const &NewMetricsList) {
  std::lock_guard<std::mutex> Lock(MetricListsMutex);
  MetricLists.emplace(SinkType, NewMetricsList);
};

Registrar Registrar::getNewRegistrar(std::string const &MetricsPrefix) {
  return {prependPrefix(MetricsPrefix), MetricLists};
}

std::string Registrar::prependPrefix(std::string const &Name) {
  return {Prefix + "." + Name};
}

} // namespace Metrics
