// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Streamer.h"
#include "DemuxTopic.h"
#include "KafkaW/PollStatus.h"
#include "Msg.h"
#include "helper.h"

namespace FileWriter {
std::chrono::milliseconds systemTime() {
  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch());
}

bool messageTimestampIsBeforeStartTimestamp(
    std::uint64_t MessageTimestamp, std::chrono::milliseconds StartTime) {
  return (
      static_cast<std::int64_t>(MessageTimestamp) <
      std::chrono::duration_cast<std::chrono::nanoseconds>(StartTime).count());
}

bool messageTimestampIsAfterStopTimestamp(std::uint64_t MessageTimestamp,
                                          std::chrono::milliseconds StopTime) {
  // Also return false if StopTime == 0, this means a stop time has not yet been
  // set
  return (StopTime.count() > 0 &&
          static_cast<std::int64_t>(MessageTimestamp) >
              std::chrono::duration_cast<std::chrono::nanoseconds>(StopTime)
                  .count());
}

Streamer::Streamer(const std::string &Broker, const std::string &TopicName,
                   StreamerOptions Opts, ConsumerPtr Consumer, DemuxPtr Demuxer)
    : Options(std::move(Opts)), ConsumerTopicName(TopicName),
      MessageProcessor(std::move(Demuxer)) {

  if (TopicName.empty() || Broker.empty()) {
    throw std::runtime_error("Missing broker or topic");
  }

  Options.BrokerSettings.KafkaConfiguration["group.id"] =
      fmt::format("filewriter--streamer--host:{}--pid:{}--topic:{}--time:{}",
                  gethostname_wrapper(), getpid_wrapper(), TopicName,
                  std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now().time_since_epoch())
                      .count());
  Options.BrokerSettings.Address = Broker;

  ConsumerInitialised = std::async(std::launch::async, &initTopic, TopicName,
                                   Options, Logger, std::move(Consumer));
}

std::pair<StreamerStatus, ConsumerPtr>
initTopic(std::string const &TopicName, StreamerOptions const &Options,
          SharedLogger const &Logger, ConsumerPtr Consumer) {
  Logger->trace("Trying to connect to \"{}\"", TopicName);
  try {
    if (Options.StartTimestamp.count() != 0) {
      Consumer->addTopicAtTimestamp(TopicName, Options.StartTimestamp -
                                                   Options.BeforeStartTime);
    } else {
      Consumer->addTopic(TopicName);
    }
    // Error if the topic cannot be found in the metadata
    if (!Consumer->topicPresent(TopicName)) {
      throw std::runtime_error(
          fmt::format("could not find topic \"{}\"", TopicName));
    }
    return {StreamerStatus::WRITING, std::move(Consumer)};
  } catch (std::exception &Error) {
    Logger->error("Initialisation failed: {}", Error.what());
    return {StreamerStatus::INITIALISATION_FAILED, nullptr};
  }
}

FileWriter::StreamerStatus FileWriter::Streamer::close() {
  RunStatus.store(StreamerStatus::HAS_FINISHED);
  return StreamerStatus::HAS_FINISHED;
}

bool Streamer::ifConsumerIsReadyThenAssignIt() {
  if (ConsumerInitialised.wait_for(std::chrono::milliseconds(100)) !=
      std::future_status::ready) {
    Logger->warn("Not yet done setting up consumer. Deferring consumption.");
    return false;
  }
  auto Temp = ConsumerInitialised.get();
  RunStatus.store(Temp.first);
  Consumer = std::move(Temp.second);
  return true;
}

bool Streamer::stopTimeExceeded() {
  if ((Options.StopTimestamp.count() > 0) and
      (systemTime() > Options.StopTimestamp + Options.AfterStopTime)) {
    Logger->info("{} ms passed since stop time in topic {}",
                 (systemTime() - Options.StopTimestamp).count(),
                 ConsumerTopicName);
    return true;
  }
  return false;
}

void Streamer::markIfOffsetsAlreadyReached(
    std::vector<std::pair<int64_t, bool>> &OffsetsToStopAt,
    std::string const &TopicName) {
  auto CurrentOffsets = Consumer->getCurrentOffsets(TopicName);
  assert(CurrentOffsets.size() == OffsetsToStopAt.size());
  for (size_t PartitionNumber = 0; PartitionNumber < CurrentOffsets.size();
       PartitionNumber++) {
    if (CurrentOffsets[PartitionNumber] >=
        OffsetsToStopAt[PartitionNumber].first) {
      OffsetsToStopAt[PartitionNumber].second = true;
    }
  }
}

/// Query Kafka brokers to find out the offset, in every partition, at which we
/// should stop consuming
std::vector<std::pair<int64_t, bool>>
Streamer::getStopOffsets(std::chrono::milliseconds StartTime,
                         std::chrono::milliseconds StopTime,
                         std::string const &TopicName) {
  // StopOffsets are a pair of the offset corresponding to the stop time and
  // whether or not that offset has been reached yet
  auto OffsetsFromStartTime =
      Consumer->offsetsForTimesAllPartitions(TopicName, StartTime);
  auto OffsetsFromStopTime =
      Consumer->offsetsForTimesAllPartitions(TopicName, StopTime);
  std::vector<std::pair<int64_t, bool>> OffsetsToStopAt(
      OffsetsFromStopTime.size(), {0, false});
  for (size_t PartitionNumber = 0; PartitionNumber < OffsetsFromStopTime.size();
       ++PartitionNumber) {
    int64_t StopOffset = OffsetsFromStopTime[PartitionNumber];
    int64_t StartOffset = OffsetsFromStartTime[PartitionNumber];
    // If stop offset is -1 that means there are not any messages after the time
    // we have asked for, so set the stop offset to the current high-watermark
    if (StopOffset == -1) {
      // -1 as the high watermark is the last available offset + 1
      StopOffset =
          Consumer->getHighWatermarkOffset(TopicName, PartitionNumber) - 1;
    }
    if (StopOffset < 0 || StartOffset < 0 || StopOffset <= StartOffset) {
      // No data on topic at all or between start and stop of run,
      // so mark this partition as stop already reached
      OffsetsToStopAt[PartitionNumber].second = true;
      Logger->debug("No data on topic {} to consume before specified stop time",
                    TopicName);
    }
    OffsetsToStopAt[PartitionNumber].first = StopOffset;
    Logger->debug(
        "Stop offset for topic {}, partition {}, is {}. Start offset was {}",
        TopicName, PartitionNumber, OffsetsToStopAt[PartitionNumber].first,
        StartOffset);
    markIfOffsetsAlreadyReached(OffsetsToStopAt, TopicName);
  }
  return OffsetsToStopAt;
}

/// Checks if the newly received message means that we have now reached the stop
/// offset for every partition.
///
/// \param NewMessagePartition partition number of the newly received message.
/// \param NewMessageOffset offset of the newly received message.
/// \return true if we've reached stop offset for every partition.
bool Streamer::stopOffsetsNowReached(int32_t NewMessagePartition,
                                     int64_t NewMessageOffset) {
  if (NewMessageOffset >= StopOffsets[NewMessagePartition].first) {
    StopOffsets[NewMessagePartition].second = true;
  }
  return stopOffsetsReached();
}

bool Streamer::stopOffsetsReached() const {
  return std::all_of(
      StopOffsets.cbegin(), StopOffsets.cend(),
      [](std::pair<int64_t, bool> const &StopPair) { return StopPair.second; });
}

bool Streamer::haveReachedStopOffsets(int32_t Partition, int64_t Offset) {
  if (!CatchingUpToStopOffset && stopTimeExceeded()) {
    CatchingUpToStopOffset = true;
    Logger->trace("Calling getStopOffsets");
    StopOffsets = getStopOffsets(Options.StartTimestamp,
                                 Options.StopTimestamp + Options.AfterStopTime,
                                 ConsumerTopicName);
    Logger->trace("Finished executing getStopOffsets");
    // There may be no data in the topic, so check if all stop offsets
    // are already marked as reached
    if (stopOffsetsReached()) {
      Logger->warn("There was no data in {} to consume", ConsumerTopicName);
      return true;
    }
  }

  if (CatchingUpToStopOffset && stopOffsetsNowReached(Partition, Offset)) {
    Logger->debug("Reached stop offsets in topic {}", ConsumerTopicName);
    return true;
  }

  return false;
}

bool Streamer::messageHasPayload(KafkaW::PollStatus MessageStatus) {
  if (MessageStatus == KafkaW::PollStatus::Error) {
    // TODO count Kafka error
    return false;
  }

  return !(MessageStatus == KafkaW::PollStatus::Empty ||
           MessageStatus == KafkaW::PollStatus::EndOfPartition ||
           MessageStatus == KafkaW::PollStatus::TimedOut);
}

bool Streamer::messageSourceIsValid(
    FlatbufferMessage::SrcHash SourceHash) const {
  return MessageProcessor->canHandleSource(SourceHash);
}

bool Streamer::messageTimestampInRange(std::uint64_t Timestamp) const {
  return !(
      messageTimestampIsBeforeStartTimestamp(Timestamp,
                                             Options.StartTimestamp) ||
      messageTimestampIsAfterStopTimestamp(Timestamp, Options.StopTimestamp));
}

std::unique_ptr<FlatbufferMessage>
Streamer::createFlatBufferMessage(char const *Data, size_t Size) {
  std::unique_ptr<FlatbufferMessage> FBMessage;
  try {
    FBMessage = std::make_unique<FlatbufferMessage>(Data, Size);
  } catch (NotValidFlatbuffer &Error) {
    ++NumberFailedValidation;
    return nullptr;
  } catch (std::runtime_error &Error) {
    // TODO catch different exceptions and increment different counters
    return nullptr;
  }

  return FBMessage;
}

void Streamer::processMessage(
    std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> &KafkaMessage) {

  if (auto const FBMessage = createFlatBufferMessage(
          KafkaMessage->second.data(), KafkaMessage->second.size())) {
    if (!messageSourceIsValid(FBMessage->getSourceHash())) {
      // TODO count unknown source name
      return;
    }

    if (!messageTimestampInRange(FBMessage->getTimestamp())) {
      return;
    }

    // TODO: Collect information about the data received

    try {
      MessageProcessor->process_message(*FBMessage);
      ++NumberProcessedMessages;
    } catch (MessageProcessingException const &Error) {
      // TODO: Log process failures?
    }
  }
}

std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> Streamer::poll() {
  if (Consumer == nullptr && ConsumerInitialised.valid()) {
    auto ready = ifConsumerIsReadyThenAssignIt();
    if (!ready) {
      // Not ready, so try again on next poll
      return nullptr;
    }
  }

  // Potentially the consumer might not be usable.
  if (RunStatus < StreamerStatus::IS_CONNECTED) {
    throw std::runtime_error(StatusDescription(RunStatus));
  }

  return Consumer->poll();
}

void Streamer::process() {
  // Consume message
  if (auto KafkaMessage = poll()) {
    // Check stop offsets
    if (haveReachedStopOffsets(KafkaMessage->second.MetaData.Partition,
                               KafkaMessage->second.MetaData.Offset)) {
      RunStatus.store(StreamerStatus::HAS_FINISHED);
      return;
    }

    if (!messageHasPayload(KafkaMessage->first)) {
      return;
    }

    processMessage(KafkaMessage);
  }
}

// cppcheck-suppress unusedFunction; used by unit tests.
void Streamer::setStartTime(std::chrono::milliseconds const &StartTime) {
  Options.StartTimestamp = StartTime;
}

void Streamer::setStopTime(std::chrono::milliseconds const &StopTime) {
  Options.StopTimestamp = StopTime;
}

} // namespace FileWriter
