// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Sink.h"
#include "InternalMetric.h"
#include <asio.hpp>
#include <memory>
#include <thread>
#include <map>
#include <mutex>

namespace Metrics {

class Reporter {
public:
  Reporter(std::unique_ptr<Sink> MetricSink, std::chrono::milliseconds Interval)
      : MetricSink(std::move(MetricSink)), IO(), Period(Interval),
        AsioTimer(IO, Period){};

  void reportMetrics() {
    std::lock_guard<std::mutex> Lock(MetricsMutex);
    for (auto &MetricNameValue : MetricsToReportOn) {
      MetricSink->reportMetric(MetricNameValue.second);
    }
  }

  void addMetric(Metric &NewMetric, std::string const &NewName) {
    std::lock_guard<std::mutex> Lock(MetricsMutex);
    if (MetricsToReportOn.find(NewName) != MetricsToReportOn.end()) {
      return;
    }
    MetricsToReportOn.emplace(NewName, InternalMetric(NewMetric, NewName));
  }

  void tryRemoveMetric(std::string const &MetricName) {
    std::lock_guard<std::mutex> Lock(MetricsMutex);
    MetricsToReportOn.erase(MetricName);
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
  std::mutex MetricsMutex;
  std::map<std::string, InternalMetric> MetricsToReportOn; // MetricName: Metric
  asio::io_context IO;
  std::chrono::milliseconds Period;
  asio::steady_timer AsioTimer;
  std::thread ReporterThread;
};
}
