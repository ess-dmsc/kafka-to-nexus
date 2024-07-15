#include "LogSink.h"
#include "InternalMetric.h"

namespace {
std::unordered_map<Metrics::Severity, int> LogSeverityMap{
    {Metrics::Severity::DEBUG, static_cast<int>(LogSeverity::Debug)},
    {Metrics::Severity::INFO, static_cast<int>(LogSeverity::Info)},
    {Metrics::Severity::WARNING, static_cast<int>(LogSeverity::Warn)},
    {Metrics::Severity::ERROR, static_cast<int>(LogSeverity::Error)},
};
} // namespace

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
    Logger::Log(
        LogSeverityMap[MetricToBeReported.ValueSeverity],
        R"(In the past {} ms, {} events of type "{}" have occurred ({}).)",
        TimeDiff, ValueDiff, MetricToBeReported.FullName,
        MetricToBeReported.DescriptionString);
  }

  MetricToBeReported.LastTime = Now;
}
} // namespace Metrics
