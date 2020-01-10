#include "Metric.h"
#include "Reporter.h"

namespace Metrics {
Metric::~Metric() {
  if (ReporterForMetric != nullptr) {
    ReporterForMetric->tryRemoveMetric(FullName);
  }
}
} // namespace Metrics
