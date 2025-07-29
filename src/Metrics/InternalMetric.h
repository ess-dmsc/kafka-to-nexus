// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Metric.h"
#include "logger.h"
#include <chrono>
#include <execinfo.h>
#include <functional>

namespace Metrics {

/// Should not be used outside of objects in the Metrics namespace
/// InternalMetric contains details we need from a Metric instance in order for
/// the Reporter to report on the Metric
struct InternalMetric {
  explicit InternalMetric(std::shared_ptr<Metric> MetricToGetDetailsFrom,
                          std::string Name)
      : Name(MetricToGetDetailsFrom->getName()), FullName(std::move(Name)),
        Counter(MetricToGetDetailsFrom->getCounterPtr()),
        DescriptionString(MetricToGetDetailsFrom->getDescription()),
        LastValue(MetricToGetDetailsFrom->getCounterPtr()->load()),
        MetricStore(MetricToGetDetailsFrom), Value([this]() {
          if (MetricStore)
            return MetricStore->getStringValue();
          return std::string{};
        }),
        ValueSeverity(MetricToGetDetailsFrom->getSeverity()) {
    Logger::Warn("Creating InternalMetric: {}", FullName);
  };
  ~InternalMetric() {
    Logger::Warn("Deleting InternalMetric: {}", this);
    Logger::Warn("with FullName: {}", FullName);

    const int max_frames = 20;
    void *frames[max_frames];
    int frame_count = backtrace(frames, max_frames);
    char **symbols = backtrace_symbols(frames, frame_count);
    if (symbols) {
      for (int i = 0; i < frame_count; ++i) {
        Logger::Warn("{}: {}", i, symbols[i]);
      }
      free(symbols);
    }
  }
  std::string const Name;
  std::string const FullName; // Including prefix from local registrar
  CounterType *Counter{nullptr};
  std::string const DescriptionString;
  std::int64_t LastValue{0};
  std::chrono::system_clock::time_point LastTime{
      std::chrono::system_clock::now()};

private:
  std::shared_ptr<Metric> MetricStore;

public:
  std::function<std::string()> Value; //	these MUST be declared AFTER
                                      // MetricStore to ensure memory safety!
  Severity const ValueSeverity;
};
} // namespace Metrics
