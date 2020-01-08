#include "Metric.h"
#include "Registrar.h"

namespace Metrics {
Metric::~Metric() {
  if (MetricsRegistrar != nullptr) {
    MetricsRegistrar->deregisterMetric(MName);
  }
}
} // namespace Metrics
