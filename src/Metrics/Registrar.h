#pragma once

#include "Reporter.h"
#include "Sink.h"
#include <map>
#include <string>
#include <vector>

namespace Metrics {

class Metric;

/// Register and deregister metrics to be reported via a specified sink
/// Manages metrics name prefixes
class Registrar {
public:
  Registrar(std::string MetricsPrefix,
            std::vector<std::shared_ptr<Reporter>> Reporters)
      : Prefix(std::move(MetricsPrefix)) {
    for (auto const &NewReporter : Reporters) {
      ReporterList.emplace(NewReporter->getSinkType(), NewReporter);
    }
  };

  void registerMetric(Metric &NewMetric, std::vector<LogTo> const &SinkTypes);

  /// Deregister from all sinks
  void deregisterMetric(std::string const &MetricName);

  Registrar getNewRegistrar(std::string const &MetricsPrefix);

private:
  std::string prependPrefix(std::string const &Name);
  std::string const Prefix;
  /// List of reporters we might want to add a metric to
  std::map<LogTo, std::shared_ptr<Reporter>> ReporterList;
};

} // namespace Metrics
