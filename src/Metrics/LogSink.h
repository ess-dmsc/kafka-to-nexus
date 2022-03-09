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
#include "logger.h"

namespace Metrics {

/// \brief Convert metrics counters into log messages.
class LogSink : public Sink {
public:
  /// \brief The function that does the actual conversion into a log message string.
  void reportMetric(InternalMetric &MetricToBeReported) override;

  /// \brief The type of metrics converter that an instance of this class represents.
  LogTo getType() override { return LogTo::LOG_MSG; };

  /// \brief Check if it the converter is healthy.
  ///
  /// Will always return yes as the this converter can never get into a bad state.
  bool isHealthy() override { return true; };
};
} // namespace Metrics
