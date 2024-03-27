#include "Registrar.h"
#include "Metric.h"
#include <algorithm>
#include <logger.h>

namespace Metrics {

void Registrar::registerMetric(Metric &NewMetric,
                               std::vector<LogTo> const &SinkTypes) const {
  if (NewMetric.getName().empty()) {
    throw std::runtime_error("Metrics cannot be registered with an empty name");
  }
  for (auto const &SinkTypeAndReporter : ReporterList) {
    if (std::find(SinkTypes.begin(), SinkTypes.end(),
                  SinkTypeAndReporter.first) != SinkTypes.end()) {
      std::string NewName = prependPrefix(NewMetric.getName());

      if (!SinkTypeAndReporter.second->addMetric(NewMetric, NewName)) {
        throw std::runtime_error(
            "Metric with same full name is already registered");
      }
      NewMetric.setDeregistrationDetails(NewName, SinkTypeAndReporter.second);
    }
  }
}

std::unique_ptr<IRegistrar>
Registrar::getNewRegistrar(std::string const &MetricsPrefix) const {
  std::vector<std::shared_ptr<Reporter>> Reporters;
  Reporters.reserve(ReporterList.size());

  for (auto const &SinkTypeAndReporter : ReporterList) {
    // cppcheck-suppress useStlAlgorithm
    Reporters.push_back(SinkTypeAndReporter.second);
  }
  return std::make_unique<Registrar>(prependPrefix(MetricsPrefix), Reporters);
}

std::string Registrar::prependPrefix(std::string const &Name) const {
  if (Prefix.empty()) {
    return Name;
  }
  return {Prefix + "." + Name};
}
} // namespace Metrics
