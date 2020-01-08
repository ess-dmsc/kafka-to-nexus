#include "Type.h"
#include "Processor.h"

namespace Metrics {
Metric::~Metric() {
  if (DeregPtr != nullptr) {
    DeregPtr->deregisterMetric(DeregName);
  }
}
} // namespace Metrics
