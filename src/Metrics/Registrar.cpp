#include "Registrar.h"
#include "Metric.h"
#include <algorithm>

namespace Metrics {

void Registrar::registerMetric(Metric &NewMetric,
                               std::vector<LogTo> const &SinkTypes) const {
  if (NewMetric.getName().empty()) {
    throw std::runtime_error("Metrics cannot be registered with an empty name");
  }
  for (auto const &reporter : ReporterList) {
    if (std::find(SinkTypes.begin(), SinkTypes.end(),
                  reporter->getSinkType()) != SinkTypes.end()) {
      std::string NewName = prependPrefix(NewMetric.getName());

      if (!reporter->addMetric(NewMetric, NewName)) {
        throw std::runtime_error(
            "Metric with same full name is already registered");
      }
      NewMetric.setDeregistrationDetails(NewName, reporter);
    }
  }
}

std::unique_ptr<IRegistrar>
Registrar::getNewRegistrar(std::string const &MetricsPrefix) const {
  return std::make_unique<Registrar>(prependPrefix(MetricsPrefix),
                                     ReporterList);
}

std::string Registrar::prependPrefix(std::string const &Name) const {
  if (Prefix.empty()) {
    return Name;
  }
  return {Prefix + "." + Name};
}
} // namespace Metrics
