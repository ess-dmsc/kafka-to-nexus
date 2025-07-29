#include "Metric.h"
#include "Reporter.h"
#include "logger.h"

namespace Metrics {

void Metric::setDeregistrationDetails(std::string const &NameWithPrefix,
                                      std::weak_ptr<Reporter> const &Reporter) {
  FullName = NameWithPrefix;
  Reporters.emplace_back(Reporter);
}

std::string Metric::getStringValue() const {
  if (Value != "")
    return Value;
  else
    return std::to_string(Counter.load(std::memory_order_relaxed));
}

Metric::~Metric() {
  for (auto const &CReporter : Reporters) {
    if (auto Reporter = CReporter.lock()) {
      if (!Reporter->tryRemoveMetric(FullName)) {
        Logger::Error("Failed to (self) remove metric: {}", MName);
      }
    }
  }
}
} // namespace Metrics
