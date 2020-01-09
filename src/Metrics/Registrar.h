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
  void registerMetric(Metric &NewMetric, std::vector<LogTo> const &SinkTypes);
  void deregisterMetric(std::string const &MetricName);
  void addMetricsList(LogTo SinkType,
                      std::shared_ptr<MetricsList> const &NewMetricsList);

private:
  std::mutex MetricListsMutex;
  /// Ownership of each MetricsList is shared with the Reporter which is
  /// responsible for it
  std::map<LogTo, std::shared_ptr<MetricsList>> MetricLists;
};

} // namespace Metrics
