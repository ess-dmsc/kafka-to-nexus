#pragma once

#include "Metrics/Type.h"

namespace Metrics {

class MetricStandIn : public Metric {
public:
  MetricStandIn(std::string Name, std::string Description, Severity Level)
      : Metric(Name, Description, Level) {}
  using Metric::getCounterPtr;
  using Metric::getDescription;
  using Metric::getName;
  using Metric::getSeverity;
  using Metric::setDeRegParams;
  using Metric::operator++;
  using Metric::operator=;
};

} // namespace Metrics
