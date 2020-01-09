#include "LogSink.h"
#include "InternalMetric.h"

namespace {
std::unordered_map<Metrics::Severity, spdlog::level::level_enum> LogSeverityMap{
    {Metrics::Severity::DEBUG, spdlog::level::level_enum::debug},
    {Metrics::Severity::INFO, spdlog::level::level_enum::info},
    {Metrics::Severity::WARNING, spdlog::level::level_enum::warn},
    {Metrics::Severity::ERROR, spdlog::level::level_enum::err}};
}

namespace Metrics {

void LogSink::reportMetric(InternalMetric &MetricToBeReported) {
  auto Now = std::chrono::system_clock::now();
  auto CurrentValue = MetricToBeReported.Counter->load();
  if (CurrentValue != 0) {
    auto ValueDiff = CurrentValue - MetricToBeReported.LastValue;
    MetricToBeReported.LastValue = CurrentValue;
    auto TimeDiff = std::chrono::duration_cast<std::chrono::milliseconds>(
                        Now - MetricToBeReported.LastTime)
                        .count();
    Logger->log(
        LogSeverityMap[MetricToBeReported.ValueSeverity],
        "In the past {} ms, {} events of type \"{}\" have occurred ({}).",
        TimeDiff, ValueDiff, MetricToBeReported.FullName,
        MetricToBeReported.DescriptionString);
  }
  MetricToBeReported.LastTime = Now;
}
}
