// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

namespace Metrics {

struct InternalMetric;

enum struct LogTo { CARBON, LOG_MSG };

class Sink {
public:
  virtual void reportMetric(InternalMetric &MetricToBeReported) = 0;
  virtual LogTo getType() = 0;
  virtual ~Sink() = default;
};

} // namespace Metrics