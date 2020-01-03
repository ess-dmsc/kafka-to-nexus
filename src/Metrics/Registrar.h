#pragma once

#include "Type.h"
#include <string>
#include <unordered_set>

namespace Metrics {

enum class LogTo { CARBON, LOG_MSG };

using DestList = std::unordered_set<LogTo>;

class ProcessorInterface;

class Processor;

class Registrar {
public:
  Registrar(Registrar &&Registrar);

  bool registerMetric(Metric &NewMetric, DestList Destinations);

  Registrar getNewRegistrar(std::string const &MetricsPrefix) const;

private:
  friend Processor;
  Registrar(std::string MetricsPrefix, ProcessorInterface *ProcessorPtr);

  std::string Prefix;
  ProcessorInterface *MetricsProcessor{nullptr};
};
} // namespace Metrics
