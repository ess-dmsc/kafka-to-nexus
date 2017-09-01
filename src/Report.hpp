#pragma once

#include <chrono>
#include <memory>
#include <thread>

#include "Status.hpp"
#include "StatusWriter.hpp"
#include "logger.h"

namespace FileWriter {

class Report {
  using ErrorCode = Status::StreamMasterErrorCode;

public:
  Report() {}
  Report(std::shared_ptr<KafkaW::ProducerTopic> producer, const int delay = 1000) : 
    report_producer_{producer}, delay_{std::chrono::milliseconds(delay)} {}
  Report(const Report& other) = delete;
  Report(Report&& other) = default;
  ~Report() = default;


  template <class S>
  void report(S& streamer,
	      std::atomic<bool>& stop, 
	      std::atomic<int>& stream_master_status) {
    while(!stop.load()) {
      std::this_thread::sleep_for(delay_);
      auto error = produce_single_report(streamer, stream_master_status.load());
      if (error == ErrorCode::report_failure) {
    	stream_master_status = error;
        return;
      }
    } 
    std::this_thread::sleep_for(delay_);
    auto error = produce_single_report(streamer, stream_master_status.load());
    if ( error == ErrorCode::report_failure) { // termination message
      stream_master_status = error;
    }
    return;
  }
  
private:

  template <class S>
  int produce_single_report(S& streamer, int stream_master_status) {
    if (!report_producer_) {
      LOG(1, "ProucerTopic error: can't produce StreamMaster status report");
      return ErrorCode::report_failure;
    }
    Status::StreamMasterStatus status(stream_master_status);
    for (auto &s : streamer) {
      auto v = s.second.status();
      status.push(s.first, v.fetch_status(), v.fetch_statistics());
    }
    auto value = Status::pprint<Status::JSONStreamWriter>(status);
    report_producer_->produce(reinterpret_cast<unsigned char *>(&value[0]),
			      value.size());
    return 0;
  }

  std::shared_ptr<KafkaW::ProducerTopic> report_producer_{nullptr};
  std::chrono::milliseconds delay_{milliseconds(1000)};
};

}
