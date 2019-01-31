#pragma once

#include <chrono>
#include <memory>
#include <thread>

#include "KafkaW/KafkaW.h"
#include "Status.h"
#include "StatusWriter.h"
#include "logger.h"

namespace FileWriter {

class Report {
  using StreamMasterError = Status::StreamMasterError;
  using ReportType = FileWriter::Status::StatusWriter;

public:
  Report() : ReportMs{std::chrono::milliseconds{1000}} {}
  Report(std::shared_ptr<KafkaW::ProducerTopic> KafkaProducer,
         const std::string &JID,
         const std::chrono::milliseconds &MsBetweenReports =
             std::chrono::milliseconds{1000})
      : Producer{KafkaProducer}, JobId(JID), ReportMs{MsBetweenReports} {}
  Report(const Report &) = delete;
  Report(Report &&) = default;
  Report &operator=(Report &&) = default;
  ~Report() = default;

  template <class S>
  void report(std::map<std::string, S> &Streamers, std::atomic<bool> &Stop,
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
  template <class S>
  StreamMasterError
  produceReport(std::map<std::string, S> &Streamers,
                std::atomic<StreamMasterError> &StreamMasterStatus) {
    std::this_thread::sleep_for(ReportMs);
    if (!Producer) {
      LOG(spdlog::level::err,
          "ProucerTopic error: can't produce StreamMaster status report");
      return StreamMasterError::REPORT_ERROR;
    }

    ReportType Reporter;
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
    Producer->produce(reinterpret_cast<unsigned char *>(&Value[0]),
                      Value.size());

    return StreamMasterError::OK;
  }
  Status::StreamMasterInfo Information;
  std::shared_ptr<KafkaW::ProducerTopic> Producer{nullptr};
  std::string JobId;
  std::chrono::milliseconds ReportMs;
};
} // namespace FileWriter
