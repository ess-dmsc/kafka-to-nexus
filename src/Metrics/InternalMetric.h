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
#include <chrono>
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
				MetricStore(MetricToGetDetailsFrom),
        Value([this]() {
          if (auto metricShared = MetricStore.lock())
            return metricShared->getStringValue();
          return std::string{};
        }),
        ValueSeverity(MetricToGetDetailsFrom->getSeverity()) {};
  std::string const Name;
  std::string const FullName; // Including prefix from local registrar
  CounterType *Counter{nullptr};
  std::string const DescriptionString;
  std::int64_t LastValue{0};
  std::chrono::system_clock::time_point LastTime{
      std::chrono::system_clock::now()};
private:
  std::weak_ptr<Metric> MetricStore;
public:
  std::function<std::string()> Value;	//	these MUST be declared AFTER MetricStore to ensure memory safety!
  Severity const ValueSeverity;

};
} // namespace Metrics
