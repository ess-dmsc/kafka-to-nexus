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

Partition::Partition(std::unique_ptr<KafkaW::ConsumerInterface> Consumer,
                     int Partition, std::string TopicName, SrcToDst const &Map,
                     MessageWriter *Writer, Metrics::Registrar RegisterMetric,
                     time_point Start, time_point Stop, duration StopLeeway,
                     duration KafkaErrorTimeout)
    : ConsumerPtr(std::move(Consumer)), PartitionID(Partition),
      Topic(std::move(TopicName)), StopTime(Stop), StopTimeLeeway(StopLeeway),
      StopTester(Stop, StopLeeway, KafkaErrorTimeout) {
  if (time_point::max() - StopTime <=
      StopTimeLeeway) { // Deal with potential overflow problem
    StopTime -= StopTimeLeeway;
  }

  for (auto &SrcDestInfo : Map) {
    MsgFilters.emplace(SrcDestInfo.Hash,
                       std::make_unique<SourceFilter>(
                           Start, Stop, Writer,
                           RegisterMetric.getNewRegistrar(
                               SrcDestInfo.getMetricsNameString())));
    MsgFilters[SrcDestInfo.Hash]->addDestinationPtr(SrcDestInfo.Destination);
  }

  RegisterMetric.registerMetric(KafkaTimeouts, {Metrics::LogTo::CARBON});
  RegisterMetric.registerMetric(
      KafkaErrors, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(MessagesReceived, {Metrics::LogTo::CARBON});
  RegisterMetric.registerMetric(MessagesProcessed, {Metrics::LogTo::CARBON});
  RegisterMetric.registerMetric(
      BadOffsets, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(
      FlatbufferErrors, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(
      BadTimestamps, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
}

void Partition::start() { addPollTask(); }

void Partition::setStopTime(time_point Stop) {
  Executor.sendWork([=]() {
    StopTester.setStopTime(Stop);
    for (auto &Filter : MsgFilters) {
      Filter.second->setStopTime(Stop);
    }
  });
}

bool Partition::hasFinished() const { return HasFinished.load(); }

void Partition::addPollTask() {
  Executor.sendLowPriorityWork([=]() { pollForMessage(); });
}

bool Partition::shouldStopBasedOnPollStatus(KafkaW::PollStatus CStatus) {
  if (StopTester.shouldStopPartition(CStatus)) {
    if (StopTester.hasErrorState()) {
      LOG_ERROR("Stopping consumption of data from Kafka in partition {} of "
                "topic {} due to error.",
                PartitionID, Topic);
    } else {
      LOG_INFO("Done consuming data from partition {} of topic {}.",
               PartitionID, Topic);
    }
    return true;
  }
  return false;
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
  case KafkaW::PollStatus::EndOfPartition: // Do nothing
    break;
  case KafkaW::PollStatus::Empty: // Do nothing
    break;
  }
  if (shouldStopBasedOnPollStatus(Msg.first)) {
    HasFinished = true;
    return;
  }

  if (KafkaW::PollStatus::Message == Msg.first) {
    processMessage(Msg.second);
    if (MsgFilters.empty() or
        Msg.second.getMetaData().timestamp() > StopTime + StopTimeLeeway) {
      LOG_INFO("Done consuming data from partition {} of topic {}.",
               PartitionID, Topic);
      HasFinished = true;
      return;
    }
  }
  addPollTask();
}

void Partition::processMessage(FileWriter::Msg const &Message) {
  if (CurrentOffset != 0 and
      CurrentOffset + 1 != Message.getMetaData().Offset) {
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
  if (CurrentFilter != MsgFilters.end() and
      CurrentFilter->second->filterMessage(std::move(FbMsg))) {
    MessagesProcessed++;
    if (CurrentFilter->second->hasFinished()) {
      MsgFilters.erase(CurrentFilter->first);
    }
  }
}

} // namespace Stream
