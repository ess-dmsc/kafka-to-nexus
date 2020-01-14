#include "Registrar.h"
#include "Metric.h"
#include <algorithm>

namespace Metrics {

void Registrar::registerMetric(Metric &NewMetric,
                               std::vector<LogTo> const &SinkTypes) {
  if (NewMetric.getName().empty()) {
    throw std::runtime_error("Metrics cannot be registered with an empty name");
  }
  for (auto &SinkTypeAndReporter : ReporterList) {
    if (std::find(SinkTypes.begin(), SinkTypes.end(),
                  SinkTypeAndReporter.first) != SinkTypes.end()) {
      std::string NewName = prependPrefix(NewMetric.getName());
      NewMetric.setDeregistrationDetails(NewName, SinkTypeAndReporter.second);
      SinkTypeAndReporter.second->addMetric(NewMetric, NewName);
    }
  }
}

Registrar Registrar::getNewRegistrar(std::string const &MetricsPrefix) {
  std::vector<std::shared_ptr<Reporter>> Reporters;
  for (auto &SinkTypeAndReporter : ReporterList) {
    Reporters.push_back(SinkTypeAndReporter.second);
  }
  return {prependPrefix(MetricsPrefix), Reporters};
}

std::string Registrar::prependPrefix(std::string const &Name) {
  if (Prefix.empty()) {
    return Name;
  }
  return {Prefix + "." + Name};
}

} // namespace Metrics
