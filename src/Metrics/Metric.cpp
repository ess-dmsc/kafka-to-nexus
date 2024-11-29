#include "Metric.h"
#include "Reporter.h"
#include "logger.h"

namespace Metrics {

void Metric::setDeregistrationDetails(
    std::string const &NameWithPrefix,
    std::shared_ptr<Reporter> const &Reporter) {
  FullName = NameWithPrefix;
  Reporters.emplace_back(Reporter);
}

Metric::~Metric() {
  for (auto const &CReporter : Reporters) {
    if (CReporter != nullptr && !CReporter->tryRemoveMetric(FullName)) {
      Logger::Error("Failed to (self) remove metric: {}", FullName);
    }
  }
}
} // namespace Metrics
