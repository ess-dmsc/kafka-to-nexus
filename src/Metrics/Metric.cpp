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

std::string* Metric::getStringPtr(){
	if(Value != "") return &Value;
	else{
	  CounterStr = std::to_string(getCounterPtr()->load(std::memory_order_relaxed));
		return &CounterStr;
  }
}

Metric::~Metric() {
  for (auto const &CReporter : Reporters) {
    if (CReporter != nullptr && !CReporter->tryRemoveMetric(FullName)) {
      Logger::Error("Failed to (self) remove metric: {}", FullName);
    }
  }
}
} // namespace Metrics
