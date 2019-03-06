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
                     std::shared_ptr<spdlog::logger> Logger) {
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
                               FileWriter::StreamerOptions Opts)
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

  ConsumerCreated = std::async(std::launch::async, &FileWriter::createConsumer,
                               TopicName, Options, Logger);
}

// pass the topic by value: this allow the constructor to go out of scope
// without resulting in an error
std::pair<FileWriter::Status::StreamerStatus, FileWriter::ConsumerPtr>
FileWriter::createConsumer(std::string const &TopicName,
                           FileWriter::StreamerOptions const &Options,
                           std::shared_ptr<spdlog::logger> Logger) {
  Logger->trace("Connecting to \"{}\"", TopicName);
  try {
    FileWriter::ConsumerPtr Consumer =
        KafkaW::createConsumer(Options.BrokerSettings);
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

FileWriter::Streamer::StreamerStatus FileWriter::Streamer::closeStream() {
  Sources.clear();
  RunStatus = StreamerStatus::HAS_FINISHED;
  return (RunStatus = StreamerStatus::HAS_FINISHED);
}

bool FileWriter::Streamer::ifConsumerIsReadyThenAssignIt() {
  if (ConsumerCreated.wait_for(std::chrono::milliseconds(100)) !=
      std::future_status::ready) {
    Logger->warn("Not yet done setting up consumer. Deferring consumption.");
    return false;
  }
  auto Temp = ConsumerCreated.get();
  RunStatus = Temp.first;
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
  if (Consumer == nullptr && ConsumerCreated.valid()) {
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

  if (std::find(Sources.begin(), Sources.end(), Message->getSourceName()) ==
      Sources.end()) {
    Logger->warn("Message from topic \"{}\" has an unknown source name "
                 "(\"{}\"), ignoring.",
                 MessageProcessor.topic(), Message->getSourceName());
    return ProcessMessageResult::OK;
  }

  if (Message->getTimestamp() == 0) {
    Logger->error(
        "Message from topic \"{}\", source \"{}\" has no timestamp, ignoring",
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
    if (removeSource(Message->getSourceName())) {
      return ProcessMessageResult::STOP;
    }
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
    std::unordered_map<std::string, Source> &SourceList) {
  for (auto &Src : SourceList) {
    Logger->info("Add {} to source list", Src.first);
    Sources.push_back(Src.first);
  }
}

bool FileWriter::Streamer::removeSource(const std::string &SourceName) {
  auto Iter(std::find<std::vector<std::string>::iterator>(
      Sources.begin(), Sources.end(), SourceName));
  if (Iter == Sources.end()) {
    Logger->warn("Can't remove source {}, not in the source list", SourceName);
    return false;
  }
  Sources.erase(Iter);
  Logger->info("Remove source {}", SourceName);
  return true;
}
