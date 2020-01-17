#include "Reporter.h"
#include "logger.h"

namespace Metrics {

void Reporter::reportMetrics() {
  if (!MetricSink->isHealthy()) {
    // skip this batch of reports and give Sink time to recover
    return;
  }
  std::lock_guard<std::mutex> Lock(MetricsMapMutex);
  for (auto &MetricNameValue : MetricsToReportOn) {
    MetricSink->reportMetric(MetricNameValue.second);
  }
  AsioTimer.expires_at(AsioTimer.expires_at() + Period);
  AsioTimer.async_wait(
      [this](std::error_code const & /*error*/) { this->reportMetrics(); });
}

bool Reporter::addMetric(Metric &NewMetric, std::string const &NewName) {
  std::lock_guard<std::mutex> Lock(MetricsMapMutex);
  auto Result =
      MetricsToReportOn.emplace(NewName, InternalMetric(NewMetric, NewName));
  return Result.second;
}

bool Reporter::tryRemoveMetric(std::string const &MetricName) {
  std::lock_guard<std::mutex> Lock(MetricsMapMutex);
  return static_cast<bool>(MetricsToReportOn.erase(MetricName));
}

LogTo Reporter::getSinkType() { return MetricSink->getType(); };

void Reporter::start() {
  AsioTimer.async_wait(
      [this](std::error_code const & /*error*/) { this->reportMetrics(); });
  ReporterThread = std::thread(&Reporter::run, this);
}

void Reporter::waitForStop() {
  AsioTimer.cancel();
  IO.stop();
  ReporterThread.join();
}

Reporter::~Reporter() {
  std::lock_guard<std::mutex> Lock(MetricsMapMutex);
  if (!MetricsToReportOn.empty()) {
    auto Logger = getLogger();
    std::string NamesOfMetricsStillRegistered;
    for (auto &NameMetricPair : MetricsToReportOn) {
      NamesOfMetricsStillRegistered += NameMetricPair.first;
      NamesOfMetricsStillRegistered += ", ";
    }
    Logger->error(
        "Reporter is being cleaned up, but still has the following metrics "
        "registered to it: {}",
        NamesOfMetricsStillRegistered);
  }

  waitForStop();
}
} // namespace Metrics
