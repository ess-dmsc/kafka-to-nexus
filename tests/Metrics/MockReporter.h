// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include <Metrics/Reporter.h>
#include <trompeloeil.hpp>

namespace Metrics {

class MockReporter : public Reporter {
public:
  using Reporter::Reporter;
  MAKE_MOCK2(addMetric, bool(Metric &, std::string const &), override);
  MAKE_MOCK1(tryRemoveMetric, bool(std::string const &), override);
};
} // namespace Metrics
