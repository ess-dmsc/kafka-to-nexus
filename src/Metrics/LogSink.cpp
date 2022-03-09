#include "LogSink.h"
#include "InternalMetric.h"

namespace {
std::unordered_map<Metrics::Severity, Log::Severity> const LogSeverityMap{
    {Metrics::Severity::DEBUG, Log::Severity::Debug},
    {Metrics::Severity::INFO, Log::Severity::Info},
    {Metrics::Severity::WARNING, Log::Severity::Warning},
    {Metrics::Severity::ERROR, Log::Severity::Error}};
}

namespace Metrics {

void LogSink::reportMetric(InternalMetric &MetricToBeReported) {
  auto Now = std::chrono::system_clock::now();
  auto CurrentValue =
      MetricToBeReported.Counter->load(std::memory_order_relaxed);
  auto ValueDiff = CurrentValue - MetricToBeReported.LastValue;
  if (ValueDiff != 0) {
    MetricToBeReported.LastValue = CurrentValue;
    auto TimeDiff = std::chrono::duration_cast<std::chrono::milliseconds>(
                        Now - MetricToBeReported.LastTime)
                        .count();
    Log::FmtMsg(
        LogSeverityMap[MetricToBeReported.ValueSeverity],
        "In the past {} ms, {} events of type \"{}\" have occurred ({}).",
        TimeDiff, ValueDiff, MetricToBeReported.FullName,
        MetricToBeReported.DescriptionString);
  }
  MetricToBeReported.LastTime = Now;
}
} // namespace Metrics
