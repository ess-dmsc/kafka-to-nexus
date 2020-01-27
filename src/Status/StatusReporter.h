// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "logger.h"
#include "../KafkaW/ProducerTopic.h"
#include "StatusInfo.h"
#include <asio.hpp>
#include <chrono>
#include <mutex>

namespace Status {

class StatusReporter {
public:
  explicit StatusReporter(
      std::chrono::milliseconds Interval,
      std::unique_ptr<KafkaW::ProducerTopic> &ApplicationStatusProducerTopic)
      : IO(), Period(Interval), AsioTimer(IO, Period), Running(false),
        StatusProducerTopic(std::move(ApplicationStatusProducerTopic)) {
    this->start();
  }

  void updateStatusInfo(StatusInfo NewInfo) {
    const std::lock_guard<std::mutex> lock(StatusMutex);
    Status = NewInfo;
  }

  void resetStatusInfo() {
    updateStatusInfo({"", "", std::chrono::milliseconds(0)});
  }

  void reportStatus();

  ~StatusReporter();

private:
  void start();
  void run() { IO.run(); }
  asio::io_context IO;
  std::chrono::milliseconds Period;
  asio::steady_timer AsioTimer;
  std::atomic_bool Running;
  std::thread StatusThread;
  SharedLogger Logger = getLogger();
  std::unique_ptr<KafkaW::ProducerTopic> StatusProducerTopic;
  StatusInfo Status{};
  mutable std::mutex StatusMutex;
  /// Blocks until the timer thread has stopped
  void waitForStop();
};

} // namespace Status