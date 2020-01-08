// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "MetricsList.h"
#include "Sink.h"
#include <asio.hpp>
#include <memory>
#include <thread>

namespace Metrics {

class Reporter {
public:
  Reporter(std::unique_ptr<Sink> MetricSink,
           std::shared_ptr<MetricsList> ListOfMetrics,
           std::chrono::milliseconds Interval)
      : MetricSink(std::move(MetricSink)),
        MetricsToReportOn(std::move(ListOfMetrics)), IO(), Period(Interval),
        AsioTimer(IO, Period), Running(false){};

  void reportMetrics() {
    for (auto &MetricNameValue : MetricsToReportOn->getListOfMetrics()) {
      MetricSink->reportMetric(MetricNameValue.second);
    }
  };

  void start() {
    Running = true;
    AsioTimer.async_wait(
        [this](std::error_code const & /*error*/) { this->reportMetrics(); });
    ReporterThread = std::thread(&Reporter::run, this);
  }

  void waitForStop() {
    Running = false;
    AsioTimer.cancel();
    ReporterThread.join();
  }

private:
  void run() { IO.run(); }

  std::unique_ptr<Sink> MetricSink;
  std::shared_ptr<MetricsList> MetricsToReportOn;
  asio::io_context IO;
  std::chrono::milliseconds Period;
  asio::steady_timer AsioTimer;
  std::atomic_bool Running;
  std::thread ReporterThread;
};
}
