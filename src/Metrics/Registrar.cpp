#include "Registrar.h"
#include "Metric.h"
#include <algorithm>
#include <logger.h>

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

      if (!SinkTypeAndReporter.second->addMetric(NewMetric, NewName)) {
        throw std::runtime_error(
            "Metric with same full name is already registered");
      }
      NewMetric.setDeregistrationDetails(NewName, SinkTypeAndReporter.second);
    }
  }
}

Registrar Registrar::getNewRegistrar(std::string const &MetricsPrefix) const {
  std::vector<std::shared_ptr<Reporter>> Reporters;
  Reporters.reserve(ReporterList.size());

  for (auto &SinkTypeAndReporter : ReporterList) {
    // cppcheck-suppress useStlAlgorithm
    Reporters.push_back(SinkTypeAndReporter.second);
  }
  return {prependPrefix(MetricsPrefix), Reporters};
}

std::string Registrar::prependPrefix(std::string const &Name) const {
  if (Prefix.empty()) {
    return Name;
  }
  return {Prefix + "." + Name};
}

Registrar::Registrar(const Registrar &Other)
    : Prefix(Other.Prefix), ReporterList(Other.ReporterList) {}

Registrar &Registrar::operator=(Registrar const &Other) {
  Prefix = Other.Prefix;
  ReporterList = Other.ReporterList;
  return *this;
}

} // namespace Metrics
