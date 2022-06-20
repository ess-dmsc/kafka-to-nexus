#pragma once

#include "Reporter.h"
#include "Sink.h"
#include <map>
#include <string>
#include <vector>

namespace Metrics {

class Metric;

/// Register and metrics to be reported via a specified sink
/// Manages metrics name prefixes
/// Deregistration of metric happens via Metric's destructor
class Registrar {
public:
  Registrar(std::string MetricsPrefix,
            std::vector<std::shared_ptr<Reporter>> Reporters)
      : Prefix(std::move(MetricsPrefix)) {
    for (auto const &NewReporter : Reporters) {
      ReporterList.emplace(NewReporter->getSinkType(), NewReporter);
    }
  };

  void registerMetric(Metric &NewMetric,
                      std::vector<LogTo> const &SinkTypes) const;

  Registrar getNewRegistrar(std::string const &MetricsPrefix) const;

  Registrar(Registrar const &Other);

  Registrar &operator=(Registrar const &Other);

private:
  std::string prependPrefix(std::string const &Name) const;
  std::string Prefix;
  /// List of reporters we might want to add a metric to
  std::map<LogTo, std::shared_ptr<Reporter>> ReporterList;
};

} // namespace Metrics
