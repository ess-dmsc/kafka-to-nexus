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
  StatusReporter(
      std::chrono::milliseconds Interval,
      std::unique_ptr<KafkaW::ProducerTopic> &ApplicationStatusProducerTopic)
      : IO(), Period(Interval), AsioTimer(IO, Period), Running(false),
        StatusProducerTopic(std::move(ApplicationStatusProducerTopic)) {
    this->start();
  }

  ~StatusReporter();

  /// \brief Set the slow changing information to report.
  ///
  /// \param NewInfo The new information to report
  void updateStatusInfo(StatusInfo const & NewInfo);

  /// \brief Clear out the current information.
  ///
  /// Used when a file has finished writing
  void resetStatusInfo();

  void reportStatus();

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
  std::string createReport() const;
};

} // namespace Status