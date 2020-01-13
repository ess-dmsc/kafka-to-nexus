// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "InternalMetric.h"
#include "Sink.h"
#include <asio.hpp>
#include <map>
#include <memory>
#include <thread>

namespace Metrics {

class Reporter {
public:
  Reporter(std::unique_ptr<Sink> MetricSink, std::chrono::milliseconds Interval)
      : MetricSink(std::move(MetricSink)), IO(), Period(Interval),
        AsioTimer(IO, Period){};

  virtual ~Reporter() = default;

  void reportMetrics() {
    std::lock_guard<std::mutex> Lock(MetricsMapMutex);
    for (auto &MetricNameValue : MetricsToReportOn) {
      MetricSink->reportMetric(MetricNameValue.second);
    }
  }

  virtual bool addMetric(Metric &NewMetric, std::string const &NewName) {
    std::lock_guard<std::mutex> Lock(MetricsMapMutex);
    auto Result =
        MetricsToReportOn.emplace(NewName, InternalMetric(NewMetric, NewName));
    return Result.second;
  }

  virtual bool tryRemoveMetric(std::string const &MetricName) {
    std::lock_guard<std::mutex> Lock(MetricsMapMutex);
    return static_cast<bool>(MetricsToReportOn.erase(MetricName));
  }

  LogTo getSinkType() { return MetricSink->getType(); };

  void start() {
    AsioTimer.async_wait(
        [this](std::error_code const & /*error*/) { this->reportMetrics(); });
    ReporterThread = std::thread(&Reporter::run, this);
  }

  void waitForStop() {
    AsioTimer.cancel();
    ReporterThread.join();
  }

private:
  void run() { IO.run(); }

  std::unique_ptr<Sink> MetricSink;
  std::mutex MetricsMapMutex; // lock when accessing MetricToReportOn
  std::map<std::string, InternalMetric> MetricsToReportOn; // MetricName: Metric
  asio::io_context IO;
  std::chrono::milliseconds Period;
  asio::steady_timer AsioTimer;
  std::thread ReporterThread;
};
}
