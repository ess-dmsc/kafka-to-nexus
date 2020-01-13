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
/// Sink which doesn't do anything for use in unit tests
class MockSink : public Sink {
public:
  MAKE_MOCK1(reportMetric, void(InternalMetric &), override);
  LogTo getType() override { return LogTo::LOG_MSG; }
};

} // namespace Metrics
