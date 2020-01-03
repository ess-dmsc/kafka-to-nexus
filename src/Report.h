// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <chrono>
#include <memory>
#include <thread>

#include "KafkaW/ProducerTopic.h"
#include "Status.h"
#include "StatusWriter.h"
#include "Streamer.h"
#include "logger.h"

namespace FileWriter {

class Report {
  using StreamMasterError = Status::StreamMasterError;

public:
  Report() : ReportMs{std::chrono::milliseconds{1000}} {}
  Report(std::shared_ptr<KafkaW::ProducerTopic> KafkaProducer, std::string JID,
         const std::chrono::milliseconds &MsBetweenReports =
             std::chrono::milliseconds{1000})
      : Producer(std::move(KafkaProducer)), JobId(std::move(JID)),
        ReportMs(MsBetweenReports) {}
  Report(const Report &) = delete;
  Report(Report &&) = default;
  Report &operator=(Report &&) = default;
  ~Report() = default;

  void report(std::map<std::string, Streamer> &Streamers,
              std::atomic<bool> &Stop,
              std::atomic<StreamMasterError> &StreamMasterStatus) {

    while (!Stop.load()) {
      StreamMasterError error = produceReport(Streamers, StreamMasterStatus);
      if (error == StreamMasterError::REPORT_ERROR) {
        StreamMasterStatus = error;
        return;
      }
    }
    // produce termination message
    StreamMasterError error = produceReport(Streamers, StreamMasterStatus);
    if (error != StreamMasterError::REPORT_ERROR) {
      StreamMasterStatus = error;
    }
  }

private:
  StreamMasterError
  produceReport(std::map<std::string, Streamer> &Streamers,
                std::atomic<StreamMasterError> &StreamMasterStatus) {
    std::this_thread::sleep_for(ReportMs);
    if (!Producer) {
      Logger->error(
          "ProducerTopic error: can't produce StreamMaster status report");
      return StreamMasterError::REPORT_ERROR;
    }

    FileWriter::Status::StatusWriter Reporter;
    Reporter.setJobId(JobId);
    for (auto &Element : Streamers) {
      // Writes Streamer information in JSON format
      Reporter.write(Element.second.messageInfo(), Element.first);
      // Compute cumulative stats
      Information.add(Element.second.messageInfo());
    }
    Information.setTimeToNextMessage(ReportMs);
    Information.StreamMasterStatus = StreamMasterStatus;
    Reporter.write(Information);
    std::string Value = Reporter.getJson();
    Producer->produce(Value);

    return StreamMasterError::OK;
  }
  Status::StreamMasterInfo Information;
  std::shared_ptr<KafkaW::ProducerTopic> Producer{nullptr};
  std::string JobId;
  std::chrono::milliseconds ReportMs;
  SharedLogger Logger = getLogger();
};
} // namespace FileWriter
