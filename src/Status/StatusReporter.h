// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "StatusReporterBase.h"
#include <asio.hpp>
#include <chrono>

namespace Status {

class StatusReporter : public StatusReporterBase {
public:
  StatusReporter(std::chrono::milliseconds Interval,
                 std::unique_ptr<KafkaW::ProducerTopic> &StatusProducerTopic)
      : StatusReporterBase(Interval, std::move(StatusProducerTopic)), IO(),
        AsioTimer(IO, Interval) {
    this->start();
  }

  ~StatusReporter() override;

private:
  void start();
  void run() { IO.run(); }
  asio::io_context IO;
  asio::steady_timer AsioTimer;
  std::thread StatusThread;
  /// Blocks until the timer thread has stopped
  void waitForStop();
  void postReportStatusActions() override;
};

} // namespace Status