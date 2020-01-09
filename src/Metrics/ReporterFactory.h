// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <memory>

namespace Metrics {

class Reporter;
class Registrar;
enum struct LogTo;

/// Create a Reporter with a specified type of Sink
std::unique_ptr<Reporter>
createReporter(std::shared_ptr<Registrar> const &MetricsRegistrar,
               Metrics::LogTo SinkType, std::chrono::milliseconds Interval,
               std::string const &CarbonHost = "", uint16_t CarbonPort = 0);

} // namespace Metrics
