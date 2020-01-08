#include "Metric.h"
#include "Processor.h"

namespace Metrics {
Metric::~Metric() {
  if (MetricsRegistrar != nullptr) {
    MetricsRegistrar->deregisterMetric(MName);
  }
}
} // namespace Metrics
