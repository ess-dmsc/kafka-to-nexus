#include "Type.h"
#include "Processor.h"

namespace Metrics {
Metric::~Metric() {
  if (DeRegPtr != nullptr) {
    DeRegPtr->deRegisterMetric(DeRegName);
  }
}
}
