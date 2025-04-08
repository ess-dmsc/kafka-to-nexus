// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include <Metrics/Sink.h>
#include <trompeloeil.hpp>

namespace Metrics {

class MockSink : public Sink {
public:
  explicit MockSink(LogTo LogToSink = LogTo::LOG_MSG) : SinkType(LogToSink) {};
  MAKE_MOCK1(reportMetric, void(InternalMetric &), override);
  LogTo getType() const override { return SinkType; };
  bool isHealthy() const override { return true; };

private:
  LogTo SinkType;
};

} // namespace Metrics
