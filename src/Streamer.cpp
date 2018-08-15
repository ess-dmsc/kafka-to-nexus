#include "Streamer.h"

#include "helper.h"

namespace FileWriter {
std::chrono::milliseconds systemTime() {
  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch());
}
} // namespace FileWriter

FileWriter::Streamer::Streamer(const std::string &Broker,
                               const std::string &TopicName,
                               const FileWriter::StreamerOptions &Opts)
    : Options(Opts) {

  if (TopicName.empty() || Broker.empty()) {
    throw std::runtime_error("Missing broker or topic");
  }

  Options.Settings.ConfigurationStrings["group.id"] = TopicName;
  Options.Settings.Address = Broker;

  ConsumerCreated = std::async(std::launch::async, &FileWriter::createConsumer,
                               TopicName, Options);
}

// pass the topic by value: this allow the constructor to go out of scope
// without resulting in an error
std::pair<FileWriter::Status::StreamerStatus, FileWriter::ConsumerPtr>
FileWriter::createConsumer(std::string const TopicName,
                           FileWriter::StreamerOptions const Options) {
  LOG(Sev::Debug, "Connecting to \"{}\"", TopicName);
  try {
    FileWriter::ConsumerPtr Consumer(new KafkaW::Consumer(Options.Settings));
    if (Options.StartTimestamp.count()) {
      Consumer->addTopic(TopicName,
                         Options.StartTimestamp - Options.BeforeStartTime);
    } else {
      Consumer->addTopic(TopicName);
    }
    // Error if the topic cannot be found in the metadata
    if (!Consumer->topicPresent(TopicName)) {
      LOG(Sev::Error, "Topic \"{}\" not in broker, remove corresponding stream",
          TopicName);
      return {FileWriter::Status::StreamerStatus::TOPIC_PARTITION_ERROR,
              nullptr};
    }
    return {FileWriter::Status::StreamerStatus::WRITING, std::move(Consumer)};
  } catch (std::exception &Error) {
    LOG(Sev::Error, "{}", Error.what());
    return {FileWriter::Status::StreamerStatus::CONFIGURATION_ERROR, nullptr};
  }

  return {FileWriter::Status::StreamerStatus::UNKNOWN_ERROR, nullptr};
}

FileWriter::Streamer::StreamerError FileWriter::Streamer::closeStream() {
  Sources.clear();
  RunStatus = StreamerError::HAS_FINISHED;
  return (RunStatus = StreamerError::HAS_FINISHED);
}

FileWriter::ProcessMessageResult
FileWriter::Streamer::pollAndProcess(FileWriter::DemuxTopic &MessageProcessor) {

  try {
    // wait for connect() to finish
    if (RunStatus > StreamerError::IS_CONNECTED) {
      // Do nothing
    } else if (ConsumerCreated.valid()) {
      if (ConsumerCreated.wait_for(std::chrono::milliseconds(100)) !=
          std::future_status::ready) {
        LOG(Sev::Warning,
            "Not yet done setting up consumer. Defering consumption.");
        return ProcessMessageResult::OK;
      }
      auto Temp = ConsumerCreated.get();
      RunStatus = Temp.first;
      Consumer = std::move(Temp.second);
    } else {
      throw std::runtime_error(
          "Failed to set-up process for creating consumer.");
    }
  } catch (std::runtime_error &Error) {
    throw Error;
  } catch (std::exception &Error) {
    LOG(Sev::Critical, "Got an exception when waiting for connection: {}",
        Error.what());
    throw Error;
  }

  // make sure that the connection is ok
  // attention: connect() handles exceptions
  if (RunStatus < StreamerError::IS_CONNECTED) {
    throw std::runtime_error(Err2Str(RunStatus));
  }

  // consume message and make sure that's ok
  KafkaW::PollStatus Poll = Consumer->poll();
  if (Poll.isEmpty() || Poll.isEOP()) {
    if ((Options.StopTimestamp.count() > 0) and
        (Options.ConsumerTimeout < (systemTime() - LastMessageTimestamp))) {
      LOG(Sev::Info, "Stop stream timeout for topic \"{}\" reached. {} ms "
                     "passed since last message received.",
          MessageProcessor.topic(),
          (systemTime() - LastMessageTimestamp).count());
      Sources.clear();
      return ProcessMessageResult::STOP;
    }
    return ProcessMessageResult::OK;
  }
  if (Poll.isErr()) {
    return ProcessMessageResult::ERR;
  }

  // convert from KafkaW to Msg
  auto KafkaMessage = Poll.isMsg();
  std::unique_ptr<FlatbufferMessage> Message;
  try {
    Message.reset(new FlatbufferMessage(
        reinterpret_cast<const char *>(KafkaMessage->data()),
        KafkaMessage->size()));
  } catch (std::runtime_error &Error) {
    LOG(Sev::Warning,
        "Message that is not a valid flatbuffer encountered. The error was: {}",
        Error.what());
    return ProcessMessageResult::ERR;
  }

  LastMessageTimestamp = systemTime(); // For dealing with messsage timeouts

  if (std::find(Sources.begin(), Sources.end(), Message->getSourceName()) ==
      Sources.end()) {
    LOG(Sev::Warning, "Message from topic\"{}\" has an unknown source name "
                      "(\"{}\"), ignoring.",
        MessageProcessor.topic(), Message->getSourceName());
    return ProcessMessageResult::OK;
  }

  // Timestamp of message is before the "start" timestamp
  if (static_cast<std::int64_t>(Message->getTimestamp()) <
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Options.StartTimestamp)
          .count()) {
    return ProcessMessageResult::OK;
  }

  // Check if there is a stop stime configured and the message timestamp is
  // greater than it
  if (Options.StopTimestamp.count() > 0 &&
      static_cast<std::int64_t>(Message->getTimestamp()) >
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              Options.StopTimestamp)
              .count()) {
    if (removeSource(Message->getSourceName())) {
      return ProcessMessageResult::STOP;
    }
    return ProcessMessageResult::ERR;
  }

  // Collect information about the data received
  MessageInfo.newMessage(Message->size());

  // Write the message. Log any error and return the result of processing
  ProcessMessageResult result =
      MessageProcessor.process_message(*Message.get());
  LOG(Sev::Debug, "Processed: {}::{}", MessageProcessor.topic(),
      Message->getSourceName());
  if (ProcessMessageResult::OK != result) {
    MessageInfo.error();
  }
  return result;
}

void FileWriter::Streamer::setSources(
    std::unordered_map<std::string, Source> &SourceList) {
  for (auto &Src : SourceList) {
    LOG(Sev::Info, "Add {} to source list", Src.first);
    Sources.push_back(Src.first);
  }
}

bool FileWriter::Streamer::removeSource(const std::string &SourceName) {
  auto Iter(std::find<std::vector<std::string>::iterator>(
      Sources.begin(), Sources.end(), SourceName));
  if (Iter == Sources.end()) {
    LOG(Sev::Warning, "Can't remove source {}, not in the source list",
        SourceName);
    return false;
  }
  Sources.erase(Iter);
  LOG(Sev::Info, "Remove source {}", SourceName);
  return true;
}

/// Method that parse the json configuration and parse the options to be used
/// in RdKafka::Config
void FileWriter::StreamerOptions::setRdKafkaOptions(
    nlohmann::json const &Options) {
  if (!Options.is_object()) {
    LOG(Sev::Warning, "Unable to parse steamer options");
    return;
  }
  for (auto Option = Options.begin(); Option != Options.end(); ++Option) {
    if (Option.value().is_string()) {
      Settings.ConfigurationStrings[Option.key()] =
          Option.value().get<std::string>();
    } else if (Option.value().is_number_integer()) {
      Settings.ConfigurationIntegers[Option.key()] =
          Option.value().get<int64_t>();
    }
  }
}

/// Method that parse the json configuration and sets the parameters used in
/// the Streamer
void FileWriter::StreamerOptions::setStreamerOptions(
    nlohmann::json const &Options) {
  if (!Options.is_object()) {
    LOG(Sev::Warning, "Unable to parse steamer options");
    return;
  }
  for (auto Option = Options.begin(); Option != Options.end(); ++Option) {
    if (Option.value().is_number_integer()) {
      auto Value = Option.value().get<int64_t>();
      LOG(Sev::Info, "Set {}: {}", Option.key(), Value);
      if (Option.key() == "ms-before-start") {
        BeforeStartTime = std::chrono::milliseconds(Value);
      }
      if (Option.key() == "ms-after-stop") {
        AfterStopTime = std::chrono::milliseconds(Value);
      }
      if (Option.key() == "consumer-timeout-ms") {
        ConsumerTimeout = std::chrono::milliseconds(Value);
      }
      if (Option.key() == "metadata-retry") {
        NumMetadataRetry = Value;
      }
    } else if (Option.value().is_string()) {
      Settings.ConfigurationStrings[Option.key()] =
          Option.value().get<std::string>();
    }
  }
}
