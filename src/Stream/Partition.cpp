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

Partition::Partition(std::shared_ptr<Kafka::ConsumerInterface> Consumer,
                     int Partition, std::string TopicName, SrcToDst const &Map,
                     MessageWriter *Writer, Metrics::IRegistrar *RegisterMetric,
                     time_point Start, time_point Stop, duration StopLeeway,
                     duration KafkaErrorTimeout,
                     std::function<bool()> AreStreamersPausedFunction)
    : ConsumerPtr(std::move(Consumer)), PartitionID(Partition),
      Topic(std::move(TopicName)), StopTime(Stop), StopTimeLeeway(StopLeeway),
      StopTester(Stop, StopLeeway, KafkaErrorTimeout),
      AreStreamersPausedFunction(AreStreamersPausedFunction) {
  // Stop time is reduced if it is too close to max to avoid overflow.
  if (time_point::max() - StopTime <= StopTimeLeeway) {
    StopTime -= StopTimeLeeway;
  }
  std::map<FileWriter::FlatbufferMessage::SrcHash,
           std::unique_ptr<SourceFilter>>
      TempFilterMap;
  std::map<FileWriter::FlatbufferMessage::SrcHash,
           FileWriter::FlatbufferMessage::SrcHash>
      WriterToSourceHashMap;
  for (auto const &SrcDestInfo : Map) {
    // Note that the cppcheck warning we are suppressing here is an actual
    // false positive due to side effects of instantiating the SourceFilter
    if (TempFilterMap.find(SrcDestInfo.WriteHash) == TempFilterMap.end()) {
      TempFilterMap.emplace(SrcDestInfo.WriteHash,
                            // cppcheck-suppress stlFindInsert
                            std::make_unique<SourceFilter>(
                                Start, Stop,
                                SrcDestInfo.AcceptsRepeatedTimestamps, Writer,
                                RegisterMetric->getNewRegistrar(
                                    SrcDestInfo.getMetricsNameString())));
    }
    TempFilterMap[SrcDestInfo.WriteHash]->add_writer_module_for_message(
        SrcDestInfo.Destination);
    WriterToSourceHashMap[SrcDestInfo.WriteHash] = SrcDestInfo.SrcHash;
  }
  for (auto &Item : TempFilterMap) {
    auto UsedHash = WriterToSourceHashMap[Item.first];
    MsgFilters.emplace_back(UsedHash, std::move(Item.second));
  }

  RegisterMetric->registerMetric(KafkaTimeouts, {Metrics::LogTo::CARBON});
  RegisterMetric->registerMetric(
      KafkaErrors, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric->registerMetric(EndOfPartition, {Metrics::LogTo::CARBON});
  RegisterMetric->registerMetric(MessagesReceived, {Metrics::LogTo::CARBON});
  RegisterMetric->registerMetric(MessagesProcessed, {Metrics::LogTo::CARBON});
  RegisterMetric->registerMetric(
      BadOffsets, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric->registerMetric(
      FlatbufferErrors, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric->registerMetric(
      BadFlatbufferTimestampErrors,
      {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric->registerMetric(
      UnknownFlatbufferIdErrors,
      {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric->registerMetric(
      NotValidFlatbufferErrors,
      {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  RegisterMetric->registerMetric(
      BufferTooSmallErrors, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
}

void Partition::start() { addPollTask(); }

void Partition::forceStop() { StopTester.forceStop(); }

void Partition::sleep(const duration Duration) const {
  std::this_thread::sleep_for(Duration);
}

void Partition::stop() {
  Executor.sendLowPriorityWork([=]() { forceStop(); });
  Executor.sendWork([=]() { forceStop(); });
}

void Partition::setStopTime(time_point Stop) {
  Executor.sendWork([=]() {
    StopTime = Stop;
    StopTester.setStopTime(Stop);
    for (auto &Filter : MsgFilters) {
      Filter.second->set_stop_time(Stop);
    }
  });
}

bool Partition::hasFinished() const { return HasFinished.load(); }

void Partition::addPollTask() {
  Executor.sendLowPriorityWork([=]() { pollForMessage(); });
}

void Partition::checkAndLogPartitionTimeOut() {
  if (StopTester.hasTopicTimedOut()) {
    if (not PartitionTimeOutLogged) {
      Logger::Info(
          "No new messages were received from Kafka in partition {} of "
          "topic {} ({:.1f}s passed) when polling for new data.",
          PartitionID, Topic,
          double(std::chrono::duration_cast<std::chrono::milliseconds>(
                     system_clock::now() - StopTester.getStatusOccurrenceTime())
                     .count()) /
              1000.0);
      PartitionTimeOutLogged = true;
    }
  } else {
    PartitionTimeOutLogged = false;
  }
}

bool Partition::hasStopBeenRequested() const {
  return StopTester.hasForceStopBeenRequested();
}

bool Partition::shouldStopBasedOnPollStatus(Kafka::PollStatus CStatus) {
  checkAndLogPartitionTimeOut();
  if (StopTester.shouldStopPartition(CStatus)) {
    switch (StopTester.currentPartitionState()) {
    case PartitionFilter::PartitionState::ERROR:
      Logger::Error(
          "Stopping consumption of data from Kafka in partition {} of "
          "topic {} due to poll error.",
          PartitionID, Topic);
      break;
    case PartitionFilter::PartitionState::END_OF_PARTITION:
      Logger::Info(
          R"(Done consuming data from partition {} of topic "{}" (reached the end of the partition).)",
          PartitionID, Topic);
      break;
    case PartitionFilter::PartitionState::TIMEOUT:
    case PartitionFilter::PartitionState::DEFAULT:
    default:
      Logger::Info(R"(Done consuming data from partition {} of topic "{}".)",
                   PartitionID, Topic);
    }
    return true;
  }
  return false;
}

void Partition::pollForMessage() {
  if (hasStopBeenRequested()) {
    HasFinished = true;
    return;
  }
  if (AreStreamersPausedFunction()) {
    sleep(PauseCheckInterval);
  } else {
    auto Msg = ConsumerPtr->poll();
    switch (Msg.first) {
    case Kafka::PollStatus::Message:
      MessagesReceived++;
      break;
    case Kafka::PollStatus::EndOfPartition:
      EndOfPartition++;
      break;
    case Kafka::PollStatus::TimedOut:
      KafkaTimeouts++;
      break;
    case Kafka::PollStatus::Error:
      KafkaErrors++;
      break;
    default:
      // Do nothing
      break;
    }
    if (shouldStopBasedOnPollStatus(Msg.first)) {
      HasFinished = true;
      return;
    }

    if (Msg.first == Kafka::PollStatus::Message) {
      processMessage(Msg.second);
      if (MsgFilters.empty()) {
        Logger::Info(
            R"(Done consuming data from partition {} of topic "{}" as there are no remaining filters.)",
            PartitionID, Topic);
        HasFinished = true;
        return;
      } else if (Msg.second.getMetaData().timestamp() >
                 StopTime + StopTimeLeeway) {
        Logger::Info(
            R"(Done consuming data from partition {} of topic "{}" as we have reached the stop time. The timestamp of the last message was: {})",
            PartitionID, Topic, Msg.second.getMetaData().timestamp());
        HasFinished = true;
        return;
      }
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
  } catch (FileWriter::BufferTooSmallError &) {
    BufferTooSmallErrors++;
    FlatbufferErrors++;
    return;
  } catch (FileWriter::InvalidFlatbufferTimestamp &) {
    BadFlatbufferTimestampErrors++;
    FlatbufferErrors++;
    return;
  } catch (FileWriter::UnknownFlatbufferID &) {
    UnknownFlatbufferIdErrors++;
    FlatbufferErrors++;
    return;
  } catch (FileWriter::NotValidFlatbuffer &) {
    NotValidFlatbufferErrors++;
    FlatbufferErrors++;
    return;
  } catch (std::exception &) {
    FlatbufferErrors++;
    return;
  }
  if (std::any_of(MsgFilters.begin(), MsgFilters.end(), [&FbMsg](auto &Item) {
        return Item.first == FbMsg.getSourceHash();
      })) {
    MessagesProcessed++;
  }
  for (auto &CFilter : MsgFilters) {
    if (CFilter.first == FbMsg.getSourceHash()) {
      CFilter.second->filter_message(FbMsg);
    }
  }
  MsgFilters.erase(
      std::remove_if(MsgFilters.begin(), MsgFilters.end(),
                     [](auto &Item) { return Item.second->has_finished(); }),
      MsgFilters.end());
}

} // namespace Stream
