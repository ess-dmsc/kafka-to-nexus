#pragma once

#include "MetricsList.h"
#include "Sink.h"
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace Metrics {

class Metric;

/// Register and deregister metrics to be reported via a specified sink
/// threadsafe
class Registrar {
public:
  Registrar() = default;
  void registerMetric(Metric &NewMetric, std::vector<LogTo> const &SinkTypes);
  void deregisterMetric(std::string const &MetricName);
  void addMetricsList(LogTo SinkType,
                      std::shared_ptr<MetricsList> const &NewMetricsList);
  Registrar getNewRegistrar(std::string const &MetricsPrefix);

private:
  Registrar(std::string MetricsPrefix,
            std::map<LogTo, std::shared_ptr<MetricsList>> Metrics)
      : Prefix(std::move(MetricsPrefix)), MetricLists(std::move(Metrics)){};
  std::string prependPrefix(std::string const &Name);
  std::string Prefix;
  std::mutex MetricListsMutex;
  /// Ownership of each MetricsList is shared with the Reporter which is
  /// responsible for it
  std::map<LogTo, std::shared_ptr<MetricsList>> MetricLists;
};

} // namespace Metrics
