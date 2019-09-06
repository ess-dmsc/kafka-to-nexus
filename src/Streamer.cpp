// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Streamer.h"
#include "KafkaW/ConsumerFactory.h"
#include "KafkaW/PollStatus.h"
#include "helper.h"
#include <ciso646>

namespace FileWriter {
std::chrono::milliseconds systemTime() {
  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch());
}
bool stopTimeElapsed(std::uint64_t MessageTimestamp,
                     std::chrono::milliseconds Stoptime,
                     SharedLogger const &Logger) {
  Logger->trace("\t\tStoptime:         {}", Stoptime.count());
  Logger->trace("\t\tMessageTimestamp: {}",
                static_cast<std::int64_t>(MessageTimestamp));
  return (Stoptime.count() > 0 and
          static_cast<std::int64_t>(MessageTimestamp) >
              std::chrono::duration_cast<std::chrono::nanoseconds>(Stoptime)
                  .count());
}
} // namespace FileWriter

FileWriter::Streamer::Streamer(const std::string &Broker,
                               const std::string &TopicName,
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

  ConsumerInitialised =
      std::async(std::launch::async, &FileWriter::initTopics, TopicName,
                 Options, Logger, std::move(Consumer));
}

std::pair<FileWriter::Status::StreamerStatus, FileWriter::ConsumerPtr>
FileWriter::initTopics(std::string const &TopicName,
                       FileWriter::StreamerOptions const &Options,
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
      return {FileWriter::Status::StreamerStatus::TOPIC_PARTITION_ERROR,
              nullptr};
    }
    return {FileWriter::Status::StreamerStatus::WRITING, std::move(Consumer)};
  } catch (std::exception &Error) {
    Logger->error("{}", Error.what());
    return {FileWriter::Status::StreamerStatus::CONFIGURATION_ERROR, nullptr};
  }
}

FileWriter::Streamer::StreamerStatus FileWriter::Streamer::close() {
  Sources.clear();
  RunStatus.store(StreamerStatus::HAS_FINISHED);
  return StreamerStatus::HAS_FINISHED;
}

bool FileWriter::Streamer::ifConsumerIsReadyThenAssignIt() {
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

bool FileWriter::Streamer::stopTimeExceeded(
    FileWriter::DemuxTopic &MessageProcessor) {
  if ((Options.StopTimestamp.count() > 0) and
      (systemTime() > Options.StopTimestamp + Options.AfterStopTime)) {
    Logger->info("Stop stream timeout for topic \"{}\" reached. {} ms "
                 "passed since stop time.",
                 MessageProcessor.topic(),
                 (systemTime() - Options.StopTimestamp).count());
    Sources.clear();
    return true;
  }
  return false;
}

FileWriter::ProcessMessageResult
FileWriter::Streamer::pollAndProcess(FileWriter::DemuxTopic &MessageProcessor) {
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

  if (KafkaMessage->first == KafkaW::PollStatus::Error) {
    return ProcessMessageResult::ERR;
  }

  if (KafkaMessage->first == KafkaW::PollStatus::Empty ||
      KafkaMessage->first == KafkaW::PollStatus::EndOfPartition ||
      KafkaMessage->first == KafkaW::PollStatus::TimedOut) {
    if (stopTimeExceeded(MessageProcessor)) {
      return ProcessMessageResult::STOP;
    }
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

  if (Sources.find(Message->getSourceHash()) == Sources.end()) {
    Logger->warn("Message from topic \"{}\" with the source name \"{}\" is "
                 "unknown, ignoring.",
                 MessageProcessor.topic(), Message->getSourceName());
    return ProcessMessageResult::OK;
  }

  if (Message->getTimestamp() == 0) {
    Logger->error(
        R"(Message from topic "{}", source "{}" has no timestamp, ignoring)",
        MessageProcessor.topic(), Message->getSourceName());
    return ProcessMessageResult::ERR;
  }

  // Timestamp of message is before the "start" timestamp
  if (static_cast<std::int64_t>(Message->getTimestamp()) <
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Options.StartTimestamp)
          .count()) {
    return ProcessMessageResult::OK;
  }

  // Check if there is a stoptime configured and the message timestamp is
  // greater than it
  if (stopTimeElapsed(Message->getTimestamp(), Options.StopTimestamp, Logger)) {
    if (removeSource(Message->getSourceHash())) {
      Logger->info("Remove source {}", Message->getSourceName());
      return ProcessMessageResult::STOP;
    }
    Logger->warn("Can't remove source {}, not in the source list",
                 Message->getSourceName());
    return ProcessMessageResult::ERR;
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

void FileWriter::Streamer::setSources(
    std::unordered_map<FlatbufferMessage::SrcHash, Source> &SourceList) {
  for (auto &Src : SourceList) {
    Logger->info("Add {} to source list", Src.second.sourcename());
    Sources.emplace(Src.second.getHash(), Src.second.sourcename());
  }
}

bool FileWriter::Streamer::removeSource(FlatbufferMessage::SrcHash Hash) {
  if (Sources.find(Hash) == Sources.end()) {
    return false;
  }
  Sources.erase(Hash);
  return true;
}
