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

public:
  Report() {}
  Report(std::shared_ptr<KafkaW::ProducerTopic> producer,
         const std::chrono::milliseconds &report_ms =
             std::chrono::milliseconds{1000})
      : report_producer_{producer}, report_ms_{report_ms} {}
  Report(const Report &other) = delete;
  Report(Report &&other) = default;
  Report &operator=(Report &&other) = default;
  ~Report() = default;

  template <class S>
  void report(S &streamer, std::atomic<bool> &stop,
              std::atomic<StreamMasterError> &stream_master_status) {
    while (!stop.load()) {
      std::this_thread::sleep_for(report_ms_);
      StreamMasterError error =
          produce_single_report(streamer, stream_master_status);
      if (error == StreamMasterError::REPORT_ERROR()) {
        stream_master_status = error;
        return;
      }
    }
    std::this_thread::sleep_for(report_ms_);
    StreamMasterError error =
        produce_single_report(streamer, stream_master_status);
    if (!error.isOK()) { // termination message
      stream_master_status = error;
    }
    return;
  }

private:
  template <class S>
  StreamMasterError
  produce_single_report(S &streamer,
                        std::atomic<StreamMasterError> &stream_master_status) {
    if (!report_producer_) {
      LOG(Sev::Error,
          "ProucerTopic error: can't produce StreamMaster status report");
      return StreamMasterError::REPORT_ERROR();
    }

    //    info.status(stream_master_status);
    info.setTimeToNextMessage(report_ms_);

    for (auto &s : streamer) {
      std::lock_guard<std::mutex> Lock(s.second.messageInfo().getMutex());
      info.add(s.second.messageInfo());
    }
    Status::JSONStreamWriter::ReturnType value =
        Status::pprint<Status::JSONStreamWriter>(info);
    report_producer_->produce(reinterpret_cast<unsigned char *>(&value[0]),
                              value.size());
    return StreamMasterError::OK();
  }

  Status::StreamMasterInfo info;
  std::shared_ptr<KafkaW::ProducerTopic> report_producer_{nullptr};
  std::chrono::milliseconds report_ms_;
};
} // namespace FileWriter
