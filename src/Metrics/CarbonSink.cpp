#include "CarbonSink.h"
#include "InternalMetric.h"

namespace Metrics {

void CarbonSink::reportMetric(InternalMetric &MetricToBeReported) {
  auto CurrentValue =
      MetricToBeReported.Counter->load(std::memory_order_relaxed);
  auto CurrentName = MetricToBeReported.FullName;
  auto TimeSinceEpoch = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
  CarbonConnection.sendMessage(
      fmt::format("{} {} {}\n", CurrentName, CurrentValue, TimeSinceEpoch));
}

bool CarbonSink::isHealthy() const {
  // If it has successfully sent the previous batch of metrics then report
  // healthy and ready for a next batch
  return (CarbonConnection.messageQueueSize() == 0);
}
} // namespace Metrics
