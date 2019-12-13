#pragma once

#include <string>
#include <unordered_set>
#include "Type.h"

namespace Metrics {

enum class LogTo {
  GRAPHITE,
  LOG_MSG
};

using DestList = std::unordered_set<LogTo>;

class ProcessorInterface;

class Processor;

class Registrar {
public:
  Registrar(Registrar &&Registrar);

  bool registerMetric(Metric &NewMetric, DestList Destinations);

  Registrar getNewRegistrar(std::string MetricsPrefix);

private:
  friend Processor;
  Registrar(std::string MetricsPrefix, ProcessorInterface *Processor);

  std::string Prefix;
  ProcessorInterface *Processor{nullptr};
};
} // namespace Metrics