#include "CarbonSink.h"
#include "InternalMetric.h"

namespace Metrics {

void CarbonSink::reportMetric(InternalMetric &MetricToBeReported) {
  if (not CarbonConnection.messageQueueEmpty()) {
    Logger->warn("Not keeping up with reporting metrics to Carbon:"
                 "local queue not empty before next report iteration");
    return;
  }

  auto CurrentValue = MetricToBeReported.Counter->load();
  auto CurrentName = MetricToBeReported.Name;
  auto TimeSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
  CarbonConnection.sendMessage(
      fmt::format("{} {} {}\n", CurrentName, CurrentValue, TimeSinceEpoch));
}
}
