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

using JsonGeneratorFuncType = std::function<void(nlohmann::json &)>;

class StatusReporterBase {
public:
  StatusReporterBase(Kafka::BrokerSettings const &Settings,
                     std::string const &StatusTopic,
                     ApplicationStatusInfo const &StatusInformation)
      : Period(StatusInformation.UpdateInterval),
        Producer(std::make_shared<Kafka::Producer>(Settings)),
        StatusProducerTopic(
            std::make_unique<Kafka::ProducerTopic>(Producer, StatusTopic)),
        StaticStatusInformation(StatusInformation) {}
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
  virtual void updateStatusInfo(JobStatusInfo const &NewInfo);

  virtual void useAlternativeStatusTopic(std::string const &AltTopicName);

  virtual void revertToDefaultStatusTopic();

  /// \brief Update the stop time to be reported.
  ///
  /// \param StopTime The new stop time.
  virtual void updateStopTime(time_point StopTime);

  /// \brief Get the currently reported stop time.
  virtual time_point getStopTime();

  /// \brief Clear out the current information.
  ///
  /// Used when a file has finished writing.
  virtual void resetStatusInfo();

  /// \brief Generate a FlatBuffer serialised report.
  ///
  /// \return The report message buffer.
  virtual flatbuffers::DetachedBuffer
  createReport(std::string const &JSONReport) const;

  /// Create the JSON part of the status report.
  virtual std::string createJSONReport() const;

  virtual void
  setJSONMetaDataGenerator(JsonGeneratorFuncType GeneratorFunction) {
    std::lock_guard Lock(StatusMutex);
    JSONGenerator = GeneratorFunction;
  }

protected:
  duration const Period;
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
  std::function<void(nlohmann::json &JSONNode)> JSONGenerator;
};

} // namespace Status
