#include "Streamer.h"
#include "KafkaW/ConsumerFactory.h"
#include "KafkaW/PollStatus.h"
#include "helper.h"

namespace FileWriter {
std::chrono::milliseconds systemTime() {
  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch());
}
bool stopTimeElapsed(std::uint64_t MessageTimestamp,
                     std::chrono::milliseconds Stoptime) {
  return (Stoptime.count() > 0 &&
          static_cast<std::int64_t>(MessageTimestamp) >
              std::chrono::duration_cast<std::chrono::nanoseconds>(Stoptime)
                  .count());
}

Streamer::Streamer(const std::string &Broker, const std::string &TopicName,
                   StreamerOptions Opts, ConsumerPtr Consumer)
    : Options(std::move(Opts)) {

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

  ConsumerInitialised = std::async(std::launch::async, &initTopics, TopicName,
                                   Options, Logger, std::move(Consumer));
}

std::pair<Status::StreamerStatus, ConsumerPtr>
initTopics(std::string const &TopicName, StreamerOptions const &Options,
           SharedLogger const &Logger, ConsumerPtr Consumer) {
  Logger->trace("Connecting to \"{}\"", TopicName);
  try {
    if (Options.StartTimestamp.count() != 0) {
      Consumer->addTopicAtTimestamp(TopicName, Options.StartTimestamp -
                                                   Options.BeforeStartTime);
    } else {
      Consumer->addTopic(TopicName);
    }
    // Error if the topic cannot be found in the metadata
    if (!Consumer->topicPresent(TopicName)) {
      Logger->error("Topic \"{}\" not in broker, remove corresponding stream",
                    TopicName);
      return {Status::StreamerStatus::TOPIC_PARTITION_ERROR, nullptr};
    }
    return {Status::StreamerStatus::WRITING, std::move(Consumer)};
  } catch (std::exception &Error) {
    Logger->error("{}", Error.what());
    return {Status::StreamerStatus::CONFIGURATION_ERROR, nullptr};
  }
}

Streamer::StreamerStatus Streamer::closeStream() {
  Sources.clear();
  RunStatus = StreamerStatus::HAS_FINISHED;
  return (RunStatus = StreamerStatus::HAS_FINISHED);
}

bool Streamer::ifConsumerIsReadyThenAssignIt() {
  if (ConsumerInitialised.wait_for(std::chrono::milliseconds(100)) !=
      std::future_status::ready) {
    Logger->warn("Not yet done setting up consumer. Deferring consumption.");
    return false;
  }
  auto Temp = ConsumerInitialised.get();
  RunStatus = Temp.first;
  Consumer = std::move(Temp.second);
  return true;
}

bool Streamer::stopTimeExceeded(DemuxTopic &MessageProcessor) {
  if ((Options.StopTimestamp.count() > 0) and
      (systemTime() > Options.StopTimestamp + Options.AfterStopTime)) {
    Logger->info("{} ms passed since stop time in topic {}",
                 (systemTime() - Options.StopTimestamp).count(),
                 MessageProcessor.topic());
    return true;
  }
  return false;
}

/// Query Kafka brokers to find out the offset, in every partition, at which we
/// should stop consuming
std::vector<std::pair<int64_t, bool>>
Streamer::getStopOffsets(std::chrono::milliseconds StopTime,
                         std::string const &TopicName) {
  // StopOffsets are a pair of the offset corresponding to the stop time and
  // whether or not that offset has been reached yet
  auto Offsets = Consumer->offsetsForTimesAllPartitions(TopicName, StopTime);
  std::vector<std::pair<int64_t, bool>> OffsetsToStopAt(Offsets.size(),
                                                        {0, false});
  for (size_t PartitionNumber = 0; PartitionNumber < Offsets.size();
       ++PartitionNumber) {
    auto StopOffset = Offsets[PartitionNumber];
    // If stop offset is -1 that means there are not any messages after the time
    // we have asked for, so set the stop offset to the current high-watermark
    if (StopOffset == -1) {
      StopOffset = Consumer->getHighWatermarkOffset(TopicName, PartitionNumber);
    }
    OffsetsToStopAt[PartitionNumber].first = StopOffset;
    Logger->info("Stop offset: {}", Offsets[PartitionNumber]);
  }
  return OffsetsToStopAt;
}

/// Checks if the newly received message means that we have now reached the stop
/// offset for every partition
/// \param NewMessagePartition partition number of the newly received message
/// \param NewMessageOffset offset of the newly received message
/// \return true if we've reached stop offset for every partition
bool Streamer::stopOffsetsReached(int32_t NewMessagePartition,
                                  int64_t NewMessageOffset) {
  if (NewMessageOffset >= StopOffsets[NewMessagePartition].first) {
    StopOffsets[NewMessagePartition].second = true;
  }
  return std::all_of(
      StopOffsets.cbegin(), StopOffsets.cend(),
      [](std::pair<int64_t, bool> const &StopPair) { return StopPair.second; });
}

ProcessMessageResult Streamer::processMessage(
    DemuxTopic &MessageProcessor,
    std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> &KafkaMessage) {
  if (!CatchingUpToStopOffset && stopTimeExceeded(MessageProcessor)) {
    CatchingUpToStopOffset = true;
    StopOffsets = getStopOffsets(Options.StopTimestamp + Options.AfterStopTime,
                                 MessageProcessor.topic());
  }

  if (KafkaMessage->first == KafkaW::PollStatus::Error) {
    return ProcessMessageResult::ERR;
  }

  if (KafkaMessage->first == KafkaW::PollStatus::Empty ||
      KafkaMessage->first == KafkaW::PollStatus::EndOfPartition ||
      KafkaMessage->first == KafkaW::PollStatus::TimedOut) {
    return ProcessMessageResult::OK;
  }

  // Convert from KafkaW to FlatbufferMessage, handles validation of flatbuffer
  std::unique_ptr<FlatbufferMessage> Message;
  try {
    Message = std::make_unique<FlatbufferMessage>(KafkaMessage->second.data(),
                                                  KafkaMessage->second.size());
  } catch (std::runtime_error &Error) {
    Logger->warn("Message that is not a valid flatbuffer encountered "
                 "(msg. offset: {}). The error was: {}",
                 KafkaMessage->second.MetaData.Offset, Error.what());
    return ProcessMessageResult::ERR;
  }

  if (CatchingUpToStopOffset &&
      stopOffsetsReached(KafkaMessage->second.MetaData.Partition,
                         KafkaMessage->second.MetaData.Offset)) {
    if (removeSource(Message->getSourceName())) {
      return ProcessMessageResult::STOP;
    }
    return ProcessMessageResult::ERR;
  }

  if (std::find(Sources.begin(), Sources.end(), Message->getSourceName()) ==
      Sources.end()) {
    Logger->warn("Message from topic \"{}\" has an unknown source name "
                 "(\"{}\"), ignoring.",
                 MessageProcessor.topic(), Message->getSourceName());
    return ProcessMessageResult::OK;
  }

  if (Message->getTimestamp() == 0) {
    Logger->error(
        R"(Message from topic "{}", source "{}" has no timestamp, ignoring)",
        MessageProcessor.topic(), Message->getSourceName());
    return ProcessMessageResult::ERR;
  }

  // If timestamp of message is before the start timestamp, ignore message and
  // carry on
  if (static_cast<std::int64_t>(Message->getTimestamp()) <
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Options.StartTimestamp)
          .count()) {
    return ProcessMessageResult::OK;
  }

  // If there is a stop time set and the timestamp of message is after it,
  // ignore message and carry on
  if (stopTimeElapsed(Message->getTimestamp(), Options.StopTimestamp)) {
    return ProcessMessageResult::OK;
  }

  // Collect information about the data received
  MessageInfo.newMessage(Message->size());

  // Write the message. Log any error and return the result of processing
  ProcessMessageResult result = MessageProcessor.process_message(*Message);
  Logger->trace("Processed: {}::{}", MessageProcessor.topic(),
                Message->getSourceName());
  if (ProcessMessageResult::OK != result) {
    MessageInfo.error();
  }
  return result;
}

ProcessMessageResult Streamer::pollAndProcess(DemuxTopic &MessageProcessor) {
  if (Consumer == nullptr && ConsumerInitialised.valid()) {
    auto ready = ifConsumerIsReadyThenAssignIt();
    if (!ready) {
      // Not ready, so try again on next poll
      return ProcessMessageResult::OK;
    }
  }

  if (RunStatus < StreamerStatus::IS_CONNECTED) {
    throw std::runtime_error(Err2Str(RunStatus));
  }

  // Consume message
  std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> KafkaMessage =
      Consumer->poll();

  return processMessage(MessageProcessor, KafkaMessage);
}

void Streamer::setSources(std::unordered_map<std::string, Source> &SourceList) {
  for (auto &Src : SourceList) {
    Logger->info("Add {} to source list", Src.first);
    Sources.push_back(Src.first);
  }
}

bool Streamer::removeSource(const std::string &SourceName) {
  auto Iter = std::find(Sources.begin(), Sources.end(), SourceName);
  if (Iter == Sources.end()) {
    Logger->warn("Can't remove source {}, not in the source list", SourceName);
    return false;
  }
  Sources.erase(Iter);
  Logger->info("Remove source {}", SourceName);
  return true;
}

} // namespace FileWriter
