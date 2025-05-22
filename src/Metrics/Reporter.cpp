#include "Reporter.h"
#include "logger.h"

namespace Metrics {

void Reporter::reportMetrics() {
  if (MetricSink->isHealthy()) {
    std::lock_guard<std::mutex> Lock(MetricsMapMutex);
    for (auto &MetricNameValue : MetricsToReportOn) {
      MetricSink->reportMetric(MetricNameValue.second);
    }
  } else {
    std::string MetricsSinkName{"Unknown"};
    std::map<LogTo, std::string> SinkNameMap{
        {LogTo::CARBON, "grafana/graphite"}, {LogTo::LOG_MSG, "log-message"}};
    if (SinkNameMap.find(MetricSink->getType()) != SinkNameMap.end()) {
      MetricsSinkName = SinkNameMap[MetricSink->getType()];
    }
    Logger::Error("Unable to push metrics to the {} sink.", MetricsSinkName);
  }
  AsioTimer.expires_at(AsioTimer.expires_at() + Period);
  AsioTimer.async_wait([this](std::error_code const &Error) {
    if (Error != asio::error::operation_aborted) {
      this->reportMetrics();
    }
  });
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

LogTo Reporter::getSinkType() const { return MetricSink->getType(); }

void Reporter::start() {
  AsioTimer.async_wait([this](std::error_code const &Error) {
    if (Error != asio::error::operation_aborted) {
      this->reportMetrics();
    }
  });
  ReporterThread = std::thread(&Reporter::run, this);
}

void Reporter::waitForStop() {
  IO.stop();
  ReporterThread.join();
}

void Reporter::server() {
    for (auto &MetricNameValue : MetricsToReportOn) {
      MetricNameValue.second.Name + ": " + MetricNameValue.second.Value;
    }
}

Reporter::~Reporter() {
  {
    std::lock_guard<std::mutex> Lock(MetricsMapMutex);
    if (!MetricsToReportOn.empty()) {
      std::string NamesOfMetricsStillRegistered;
      for (auto &NameMetricPair : MetricsToReportOn) {
        NamesOfMetricsStillRegistered += NameMetricPair.first;
        NamesOfMetricsStillRegistered += ", ";
      }
      Logger::Error(
          "Reporter is being cleaned up, but still has the following metrics "
          "registered to it: {}",
          NamesOfMetricsStillRegistered);
    }
  }

  waitForStop();
}
} // namespace Metrics
