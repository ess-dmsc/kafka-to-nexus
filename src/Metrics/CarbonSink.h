// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "CarbonInterface.h"
#include "Sink.h"
#include "logger.h"

namespace Metrics {

/// \brief Metrics Grafana (Carbon) reporter/sink.
class CarbonSink : public Sink {
public:
  CarbonSink(std::string Host, uint16_t const Port)
      : CarbonConnection(std::move(Host), Port) {};
  void reportMetric(InternalMetric &MetricToBeReported) override;
  LogTo getType() const override { return LogTo::CARBON; };
  bool isHealthy() const override;

private:
  Carbon::Connection CarbonConnection;
};
} // namespace Metrics
