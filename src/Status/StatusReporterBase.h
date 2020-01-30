// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "../KafkaW/ProducerTopic.h"
#include "StatusInfo.h"
#include "logger.h"
#include <asio.hpp>
#include <chrono>
#include <mutex>

namespace Status {

class StatusReporterBase {
public:
  StatusReporterBase(
      std::chrono::milliseconds Interval,
      std::unique_ptr<KafkaW::ProducerTopic> StatusProducerTopic)
      : Period(Interval), StatusProducerTopic(std::move(StatusProducerTopic)) {}

  virtual ~StatusReporterBase() = default;

  /// \brief Set the slow changing information to report.
  ///
  /// \param NewInfo The new information to report
  void updateStatusInfo(StatusInfo const &NewInfo);

  /// \brief Update the stop time to be reported.
  ///
  /// \param StopTime The new stop time.
  void updateStopTime(std::chrono::milliseconds StopTime);

  /// \brief Clear out the current information.
  ///
  /// Used when a file has finished writing.
  void resetStatusInfo();

  /// \brief Generate a report.
  ///
  /// \return The report as stringified JSON.
  std::string createReport() const;

protected:
  std::chrono::milliseconds const Period;
  SharedLogger Logger = getLogger();
  void reportStatus();

private:
  virtual void postReportStatusActions() {};
  StatusInfo Status{};
  mutable std::mutex StatusMutex;
  std::unique_ptr<KafkaW::ProducerTopic> StatusProducerTopic;
};

} // namespace Status