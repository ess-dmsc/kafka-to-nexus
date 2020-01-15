#include "CarbonSink.h"
#include "InternalMetric.h"

namespace Metrics {

void CarbonSink::reportMetric(InternalMetric &MetricToBeReported) {
  auto CurrentValue = MetricToBeReported.Counter->load();
  auto CurrentName = MetricToBeReported.FullName;
  auto TimeSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
  CarbonConnection.sendMessage(
      fmt::format("{} {} {}\n", CurrentName, CurrentValue, TimeSinceEpoch));
}
}