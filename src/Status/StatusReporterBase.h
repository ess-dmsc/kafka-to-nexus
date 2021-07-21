// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "../Kafka/ProducerTopic.h"
#include "StatusInfo.h"
#include "URI.h"
#include "logger.h"
#include <asio.hpp>
#include <chrono>
#include <mutex>

namespace flatbuffers {
class DetachedBuffer;
}

namespace Status {

class StatusReporterBase {
public:
  StatusReporterBase(Kafka::BrokerSettings Settings, std::string StatusTopic,
                     ApplicationStatusInfo StatusInformation)
      : Period(StatusInformation.UpdateInterval),
        Producer(std::make_shared<Kafka::Producer>(Settings)),
        StatusProducerTopic(std::make_unique<Kafka::ProducerTopic>(
            Producer, StatusTopic)),
        StaticStatusInformation(StatusInformation), StatusTopicName(StatusTopic) {}
  StatusReporterBase(std::shared_ptr<Kafka::Producer> Producer,
                     std::unique_ptr<Kafka::ProducerTopic> StatusProducerTopic,
                     ApplicationStatusInfo StatusInformation)
      : Period(StatusInformation.UpdateInterval), Producer(Producer),
        StatusProducerTopic(std::move(StatusProducerTopic)),
        StaticStatusInformation(std::move(StatusInformation)) {}

  virtual ~StatusReporterBase() = default;

  /// \brief Set the slow changing information to report.
  ///
  /// \param NewInfo The new information to report
  void updateStatusInfo(JobStatusInfo const &NewInfo);

  void useAlternativeStatusTopic(std::string const &AltTopicName);

  void revertToDefaultStatusTopic();

  /// \brief Update the stop time to be reported.
  ///
  /// \param StopTime The new stop time.
  void updateStopTime(std::chrono::milliseconds StopTime);

  /// \brief Clear out the current information.
  ///
  /// Used when a file has finished writing.
  void resetStatusInfo();

  /// \brief Generate a FlatBuffer serialised report.
  ///
  /// \return The report message buffer.
  flatbuffers::DetachedBuffer createReport(std::string const &JSONReport) const;

  /// Create the JSON part of the status report.
  std::string createJSONReport() const;

protected:
  duration const Period;
  SharedLogger Logger = getLogger();
  void reportStatus();

private:
  virtual void postReportStatusActions(){};
  JobStatusInfo Status{};
  mutable std::mutex StatusMutex;
  std::shared_ptr<Kafka::Producer> Producer;
  std::unique_ptr<Kafka::ProducerTopic> StatusProducerTopic;
  std::unique_ptr<Kafka::ProducerTopic> AltStatusProducerTopic;
  bool UsingAlternativeStatusTopic{false};
  ApplicationStatusInfo const StaticStatusInformation;
  std::string const StatusTopicName;
};

} // namespace Status
