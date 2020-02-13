// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "PartitionStream.h"

PartitionStream::PartitionStream(std::unique_ptr<KafkaW::Consumer> Consumer, SrcToDst Map, Metrics::Registrar RegisterMetric,
                                 time_point Stop) : ConsumerPtr(std::move(Consumer)), DataMap(Map), StopTime(Stop) {
  RegisterMetric.registerMetric(KafkaTimeouts, {Metrics::LogTo::CARBON});
  RegisterMetric.registerMetric(KafkaErrors, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(MessagesReceived, {Metrics::LogTo::CARBON});
  RegisterMetric.registerMetric(MessagesProcessed, {Metrics::LogTo::CARBON});
  RegisterMetric.registerMetric(KafkaErrors, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(BadOffsets, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(BadTimestamps, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  Executor.SendWork([=](){
    pollForMessage();
  });
}

void PartitionStream::setStopTime(time_point Stop) {
  Executor.SendWork([=](){
    StopTime = Stop;
  });
}

bool PartitionStream::hasFinished() {
  return HasFinished.load();
}

class ExecAtEOL {
public:
  ExecAtEOL (std::function<void ()> Func) : RunFunc(Func){}
  ~ExecAtEOL(){RunFunc();}
private:
  std::function<void()> RunFunc;
};

void PartitionStream::pollForMessage() {
  ExecAtEOL Run([=](){
    Executor.SendWork([=](){
      pollForMessage();
    });
  });
//  auto Msg = ConsumerPtr->poll();
//  switch (Msg->first) {
//    case KafkaW::PollStatus::Message:
//  }

}
