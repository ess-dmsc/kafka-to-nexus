// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Partition.h"
#include "Msg.h"

namespace Stream {

Partition::Partition(std::unique_ptr<KafkaW::Consumer> Consumer,
                     SrcToDst Map,
                     Metrics::Registrar RegisterMetric,
                     time_point Stop) : ConsumerPtr(
    std::move(Consumer)), DataMap(Map), StopTime(Stop), StopTester(StopTime, 30s) {
  RegisterMetric.registerMetric(KafkaTimeouts, {Metrics::LogTo::CARBON});
  RegisterMetric.registerMetric(KafkaErrors, {Metrics::LogTo::CARBON,
                                              Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(MessagesReceived, {Metrics::LogTo::CARBON});
  RegisterMetric.registerMetric(MessagesProcessed, {Metrics::LogTo::CARBON});
  RegisterMetric.registerMetric(BadOffsets, {Metrics::LogTo::CARBON,
                                             Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(FlatbufferErrors, {Metrics::LogTo::CARBON,
                                             Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(BadTimestamps, {Metrics::LogTo::CARBON,
                                                Metrics::LogTo::LOG_MSG});
  Executor.SendWork([=]() {
    pollForMessage();
  });
}

void Partition::setStopTime(time_point Stop) {
  Executor.SendWork([=]() {
    StopTime = Stop;
  });
}

bool Partition::hasFinished() {
  return HasFinished.load();
}

void Partition::pollForMessage() {
  auto Msg = ConsumerPtr->poll();
  switch (Msg.first) {
    case KafkaW::PollStatus::Message:
      MessagesReceived++;
      break;
    case KafkaW::PollStatus::TimedOut:
      KafkaTimeouts++;
      break;
    case KafkaW::PollStatus::Error:
      KafkaErrors++;
      break;
  }
 if (StopTester.shouldStopPartition(Msg.first)) {
   HasFinished = true;
   return;
 }

 if (KafkaW::PollStatus::Message == Msg.first) {
   processMessage(Msg.second);
 }
 if (MsgFilters.empty()) {
   HasFinished = true;
   return;
 }

  Executor.SendWork([=](){
    pollForMessage();
  });
}

void Partition::processMessage(FileWriter::Msg const &Message) {
  if (CurrentOffset != 0 and CurrentOffset + 1 != Message.getMetaData().Offset) {
    BadOffsets++;
  }
  CurrentOffset = Message.getMetaData().Offset;
  FileWriter::FlatbufferMessage FbMsg;
  try {
    FbMsg = FileWriter::FlatbufferMessage(Message);
  } catch (FileWriter::FlatbufferError &E) {
    FlatbufferErrors++;
    return;
  }
  auto CurrentFilter = MsgFilters.find(FbMsg.getSourceHash());
  if (CurrentFilter != MsgFilters.end()) {
    CurrentFilter->second.filterMessage(std::move(FbMsg));
    MessagesProcessed++;
    if (CurrentFilter->second.hasFinished()) {
      MsgFilters.erase(CurrentFilter->first);
    }
  }
}

} // namespace Stream
