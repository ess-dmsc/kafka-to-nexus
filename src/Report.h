#pragma once

#include <chrono>
#include <memory>
#include <thread>

#include "KafkaW/KafkaW.h"
#include "Status.h"
#include "StatusWriter.h"
#include "logger.h"

using ReportType = FileWriter::Status::NLJSONWriter;

namespace FileWriter {

class Report {
  using StreamMasterError = Status::StreamMasterError;

public:
  Report() {}
  Report(std::shared_ptr<KafkaW::ProducerTopic> KafkaProducer,
         const std::chrono::milliseconds &MsBetweenReports =
             std::chrono::milliseconds{1000})
      : Producer{KafkaProducer}, ReportMs{MsBetweenReports} {}
  Report(const Report &) = delete;
  Report(Report &&) = default;
  Report &operator=(Report &&) = default;
  ~Report() = default;

  template <class S>
  void report(std::map<std::string, S> &Streamers, std::atomic<bool> &Stop,
              std::atomic<StreamMasterError> &StreamMasterStatus) {
    while (!Stop.load()) {

      StreamMasterError error = produceReport(Streamers, StreamMasterStatus);
      if (error == StreamMasterError::REPORT_ERROR()) {
        StreamMasterStatus = error;
        return;
      }
    }
    // produce termination message
    StreamMasterError error = produceReport(Streamers, StreamMasterStatus);
    if (error != StreamMasterError::REPORT_ERROR()) {
      StreamMasterStatus = error;
    }
    return;
  }

private:
  template <class S>
  StreamMasterError
  produceReport(std::map<std::string, S> &Streamers,
                std::atomic<StreamMasterError> &StreamMasterStatus) {
    std::this_thread::sleep_for(ReportMs);
    if (!Producer) {
      LOG(Sev::Error,
          "ProucerTopic error: can't produce StreamMaster status report");
      return StreamMasterError::REPORT_ERROR();
    }

    Informations.setTimeToNextMessage(ReportMs);
    Informations.StreamMasterStatus = StreamMasterStatus;
    for (auto &Element : Streamers) {
      // Lock in method add makes sure that value can not be modified while
      // added, but don't care about syncronisation among different Streamer
      Informations.add(Element.second.messageInfo());
    }
    // Status::JSONStreamWriter::ReturnType value =
    //     Status::pprint<Status::JSONStreamWriter>(info);
    // report_producer_->produce(reinterpret_cast<unsigned char *>(&value[0]),
    //                           value.size());
    return StreamMasterError::OK();
  }
  Status::StreamMasterInfo Informations;
  std::shared_ptr<KafkaW::ProducerTopic> Producer{nullptr};
  std::chrono::milliseconds ReportMs;
};
} // namespace FileWriter
