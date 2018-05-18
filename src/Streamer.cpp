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
  Options.Settings.ConfigurationStrings["auto.create.topics.enable"] = "false";
  Options.Settings.Address = Broker;

  IsConnected =
      std::async(std::launch::async, &FileWriter::Streamer::Streamer::connect,
                 this, TopicName);
}

// pass the topic by value: this allow the constructor to go out of scope
// without resulting in an error
FileWriter::Streamer::StreamerError
FileWriter::Streamer::connect(std::string TopicName) {

  LOG(Sev::Debug, "Connecting to {}", TopicName);
  try {
    Consumer.reset(new KafkaW::Consumer(Options.Settings));
    if (Options.StartTimestamp.count()) {
      Consumer->addTopic(TopicName,
                         Options.StartTimestamp - Options.BeforeStartTime);
    } else {
      Consumer->addTopic(TopicName);
    }
    // Error if the topic cannot be found in the metadata
    if (!Consumer->topicPresent(TopicName)) {
      LOG(Sev::Error, "Topic {} not in broker, remove corresponding stream",
          TopicName);
      return StreamerError::TOPIC_PARTITION_ERROR();
    }
  } catch (std::exception &Error) {
    LOG(Sev::Error, "{}", Error.what());
    return StreamerError::CONFIGURATION_ERROR();
  }

  return StreamerError::WRITING();
}

FileWriter::Streamer::StreamerError FileWriter::Streamer::closeStream() {
  Sources.clear();
  RunStatus = StreamerError::HAS_FINISHED();
  return (RunStatus = StreamerError::HAS_FINISHED());
}

template <>
FileWriter::ProcessMessageResult
FileWriter::Streamer::write(FileWriter::DemuxTopic &MessageProcessor) {

  try {
    // wait for connect() to finish
    if (IsConnected.valid()) {
      if (IsConnected.wait_for(std::chrono::milliseconds(100)) !=
          std::future_status::ready) {
        LOG(Sev::Critical, "... still not ready");
        return ProcessMessageResult::OK();
      }
      RunStatus = IsConnected.get();
    }
  } catch (std::exception &Error) {
    LOG(Sev::Critical, "{}", Error.what());
  }

  // make sure that the connection is ok
  // attention: connect() handles exceptions
  if (!RunStatus.connectionOK()) {
    throw std::runtime_error(Err2Str(RunStatus));
  }

  // consume message and make sure that's ok
  KafkaW::PollStatus Poll = Consumer->poll();
  if (Poll.isEmpty() || Poll.isEOP()) {
    if ((Options.StopTimestamp.count() > 0) &&
        (systemTime() > (Options.StopTimestamp + Options.AfterStopTime))) {
      LOG(Sev::Info, "Close topic {} after time expired",
          MessageProcessor.topic());
      Sources.clear();
      return ProcessMessageResult::STOP();
    }
    return ProcessMessageResult::OK();
  }
  if (Poll.isErr()) {
    return ProcessMessageResult::ERR();
  }

  // convert from KafkaW to Msg
  Msg Message(Msg::fromKafkaW(std::move(Poll.isMsg())));
  if (Message.type == MsgType::Invalid) {
    return ProcessMessageResult::ERR();
  }

  // Make sure that the message source is relevant and that the message is in
  // the correct time window
  DemuxTopic::DT MessageTime =
      MessageProcessor.time_difference_from_message(Message);
  if (std::find(Sources.begin(), Sources.end(), MessageTime.sourcename) ==
      Sources.end()) {
    return ProcessMessageResult::OK();
  }
  if (MessageTime.dt < std::chrono::duration_cast<std::chrono::nanoseconds>(
                           Options.StartTimestamp)
                           .count()) {
    return ProcessMessageResult::OK();
  }
  if (Options.StopTimestamp.count() > 0 &&
      MessageTime.dt > std::chrono::duration_cast<std::chrono::nanoseconds>(
                           Options.StopTimestamp)
                           .count()) {
    if (removeSource(MessageTime.sourcename)) {
      return ProcessMessageResult::STOP();
    }
    return ProcessMessageResult::ERR();
  }

  // Collect information about the data received
  MessageInfo.message(Message.size());

  // Write the message. Log any error and return the result of processing
  ProcessMessageResult result =
      MessageProcessor.process_message(std::move(Message));
  LOG(Sev::Debug, "Processed: {}::{}\tpulse_time: {}", MessageProcessor.topic(),
      MessageTime.sourcename, result.ts());
  if (!result.is_OK()) {
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
/// in
/// RdKafka::Config
void FileWriter::StreamerOptions::setRdKafkaOptions(
    const rapidjson::Value *Opt) {

  if (!Opt->IsObject()) {
    LOG(Sev::Warning, "Unable to parse steamer options");
    return;
  }

  for (auto &m : Opt->GetObject()) {
    if (m.value.IsString()) {
      Settings.ConfigurationStrings[m.name.GetString()] = m.value.GetString();
      continue;
    }
    if (m.value.IsInt()) {
      Settings.ConfigurationIntegers[m.name.GetString()] = m.value.GetInt();
      continue;
    }
  }
}

/// Method that parse the json configuration and sets the parameters used in
/// the
/// Streamer
void FileWriter::StreamerOptions::setStreamerOptions(
    const rapidjson::Value *Opt) {

  if (!Opt->IsObject()) {
    LOG(Sev::Warning, "Unable to parse steamer options");
    return;
  }

  for (auto &m : Opt->GetObject()) {
    if (m.name.IsString()) {
      if (strncmp(m.name.GetString(), "ms-before-start", 15) == 0) {
        if (m.value.IsInt()) {
          LOG(Sev::Info, "Set {}: {}", m.name.GetString(), m.value.GetInt());
          BeforeStartTime = std::chrono::milliseconds(m.value.GetInt());
          continue;
        }
        LOG(Sev::Warning, "{} : wrong format", m.name.GetString());
      }
      if (strncmp(m.name.GetString(), "ms-after-stop", 13) == 0) {
        if (m.value.IsInt()) {
          LOG(Sev::Info, "Set {}: {}", m.name.GetString(), m.value.GetInt());
          AfterStopTime = std::chrono::milliseconds(m.value.GetInt());
          continue;
        }
        LOG(Sev::Warning, "{} : wrong format", m.name.GetString());
      }
      if (strncmp(m.name.GetString(), "consumer-timeout-ms", 19) == 0) {
        if (m.value.IsInt()) {
          LOG(Sev::Info, "Set {}: {}", m.name.GetString(), m.value.GetInt());
          ConsumerTimeout = std::chrono::milliseconds(m.value.GetInt());
          continue;
        }
        LOG(Sev::Warning, "{} : wrong format", m.name.GetString());
      }
      if (strncmp(m.name.GetString(), "metadata-retry", 14) == 0) {
        if (m.value.IsInt()) {
          LOG(Sev::Info, "Set {}: {}", m.name.GetString(), m.value.GetInt());
          NumMetadataRetry = m.value.GetInt();
          continue;
        }
        LOG(Sev::Warning, "{} : wrong format", m.name.GetString());
      }
      LOG(Sev::Warning, "Unknown option {}, ignore", m.name.GetString());
    }
  }
}
