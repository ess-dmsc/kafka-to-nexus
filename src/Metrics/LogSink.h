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

class LogSink : public Sink {
public:
  void reportMetric(InternalMetric &MetricToBeReported) override;
  LogTo getType() override { return LogTo::LOG_MSG; };
  bool isHealthy() override { return true; };
  SharedLogger Logger = getLogger();
};
} // namespace Metrics
