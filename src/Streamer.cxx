#include "Streamer.h"

#include "helper.h"
#include "logger.h"

#include <librdkafka/rdkafkacpp.h>

#include <algorithm>
#include <memory>

template <class T> class XType;

namespace FileWriter {
std::chrono::milliseconds systemTime() {
  using namespace std::chrono;
  system_clock::time_point now = system_clock::now();
  return duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
}
} // namespace FileWriter

/// Create and configure the RdKafka configuration used in the Streamer. If the
/// RdKafka fails in the creations of the configuration log the error, set
/// RunStatus to SEC::configuration_error and return an empty pointer. Else
/// return a unique_ptr pointing to the configuration. The configuration is
/// created using the options described in Options. For the list of available
/// options see
/// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md . If an
/// invalid option is passed log a warning.
/// \param Options an instance of StreamerOptions that contains the
/// RdKafka::Conf options.
std::unique_ptr<RdKafka::Conf> FileWriter::Streamer::createConfiguration(
    const FileWriter::StreamerOptions &Options) {

  std::unique_ptr<RdKafka::Conf> Conf(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!Conf) {
    LOG(Sev::Error, "Error: invalid configuration");
    RunStatus = SEC::configuration_error;
    return nullptr;
  }

  std::string ErrStr;
  for (std::pair<std::string, std::string> Option : Options.RdKafkaOptions) {
    if (Conf->set(Option.first, Option.second, ErrStr) !=
        RdKafka::Conf::CONF_OK) {
      LOG(Sev::Warning, "{}", ErrStr);
    }
  }
  return Conf;
}

/// Create the RdKafka::KafkaConsumer using the specified RdKafka::Conf
/// configuration and store the consumer in a unique_ptr. If the creation
/// succeed return SEC::no_error, else SEC::consumer_error
/// \param conf the RdKafka configuration to be used to create the consumer
FileWriter::Status::StreamerErrorCode
FileWriter::Streamer::createConsumer(std::unique_ptr<RdKafka::Conf> &&Conf) {
  std::string ErrStr;
  Consumer.reset(RdKafka::KafkaConsumer::create(Conf.get(), ErrStr));
  if (!Consumer) {
    LOG(Sev::Error, "{}", ErrStr);
    return SEC::consumer_error;
  }
  return SEC::no_error;
}

/// Retrieves the RdKafka::Matadata associated with the consumer and return it
/// as a unique_ptr. If fail log the error, set the RunStatus to
/// SEC::metadata_error and return a null pointer.
std::unique_ptr<RdKafka::Metadata> FileWriter::Streamer::createMetadata() {
  RdKafka::Metadata *Metadata{ nullptr };
  std::unique_ptr<RdKafka::Topic> Topic;
  int retry{ 0 };
  auto err = Consumer->metadata(Topic != nullptr, Topic.get(), &Metadata, 1000);
  while (err != RdKafka::ERR_NO_ERROR && retry < Options.NumMetadataRetry) {
    err = Consumer->metadata(Topic != nullptr, Topic.get(), &Metadata, 1000);
    ++retry;
  }
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Error, "{}", RdKafka::err2str(err));
    RunStatus = SEC::metadata_error;
    return std::unique_ptr<RdKafka::Metadata>{ nullptr };
  } else {
    return std::unique_ptr<RdKafka::Metadata>{ Metadata };
  }
}

/// Reads the metadata structure and fills the RdKafka::TopicPartition vector
/// with all the partitions corresponding to the given topic
/// \param metadata
FileWriter::Status::StreamerErrorCode
FileWriter::Streamer::createTopicPartition(
    const std::string &TopicName,
    std::unique_ptr<RdKafka::Metadata> &&Metadata) {
  if (RunStatus == SEC::metadata_error || !Metadata) {
    return SEC::metadata_error;
  }
  using PartitionMetadataVector =
      std::vector<const RdKafka::PartitionMetadata *>;
  const PartitionMetadataVector *pmv{ nullptr };
  for (auto &t : *Metadata->topics()) {
    if (t->topic() == TopicName) {
      pmv = t->partitions();
      break;
    }
  }
  if (!pmv) {
    LOG(Sev::Error, "Error: unable to find partition for topic {}", TopicName);
    return SEC::topic_partition_error;
  }
  if (!pmv->empty()) {
    for (auto p : *pmv) {
      pushTopicPartition(TopicName, p->id());
      if (!TopicPartitionVector.back()) {
        LOG(Sev::Error, "Error: unable to create partition {} for topic {}",
            p->id(), TopicName);
        return SEC::topic_partition_error;
      }
    }
  } else {
    LOG(Sev::Error, "Error: no partitions for topic {}", TopicName);
    return SEC::topic_partition_error;
  }

  return SEC::no_error;
}

/// Givent the (topic,partition) pair extracted from the metadata structure push
/// a RdKafka::TopicPartition object into the TopicPartitionVector. If a start
/// timestamp is specified in the Options object the TopicPartition is
/// initialised at the correct log, else the TopicPartition points to
/// RdKafka::Topic::OFFSET_END
/// \param topic name of the topic to listen
/// \param partition index of the partition
void FileWriter::Streamer::pushTopicPartition(const std::string &TopicName,
                                              const int32_t &Partition) {
  if (Options.StartTimestamp.count()) {
    std::chrono::milliseconds ActualStartTime =
        Options.StartTimestamp - Options.BeforeStartTime;
    TopicPartitionVector.push_back(RdKafka::TopicPartition::create(
        TopicName, Partition, ActualStartTime.count()));
  } else {
    TopicPartitionVector.push_back(RdKafka::TopicPartition::create(
        TopicName, Partition, RdKafka::Topic::OFFSET_END));
  }
}

/// Assign the RdKafka::TopicPartition vector to the consumer at the correct
/// point in time. If the Consumer is not allocated, the TopicPartitionVector
/// empty (e.g. the topic is not found) or can't assign the consumer return
/// SEC::topic_partition_error, else SEC::no_error
FileWriter::Streamer::SEC FileWriter::Streamer::assignTopicPartition() {
  if (!Consumer || TopicPartitionVector.empty()) {
    return SEC::topic_partition_error;
  }
  auto err = Consumer->offsetsForTimes(TopicPartitionVector, 1000);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Error,
        "Error while look up the offsets for the given timestamp: {}",
        RdKafka::err2str(err));
    return SEC::topic_partition_error;
  }
  err = Consumer->assign(TopicPartitionVector);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Error, "Error while assign to topic-partition: {}",
        RdKafka::err2str(err));
    return SEC::topic_partition_error;
  }
  return SEC::no_error;
};

FileWriter::Streamer::Streamer(const std::string &Broker,
                               const std::string &TopicName,
                               const FileWriter::StreamerOptions &Opts)
    : RunStatus{ SEC::not_initialized }, Options(Opts) {

  if (TopicName.empty() || Broker.empty()) {
    LOG(Sev::Error, "Broker and topic required");
    RunStatus = SEC::not_initialized;
    return;
  }
  Options.RdKafkaOptions.emplace_back("metadata.broker.list", Broker);
  Options.RdKafkaOptions.emplace_back("api.version.request", "true");
  Options.RdKafkaOptions.emplace_back("group.id", TopicName);

  Settings.ConfigurationStrings["group.id"] = TopicName;
  Settings.ConfigurationStrings["debug"] = "all";

  const int MiB = 1024 * 1024;
  Settings.ConfigurationIntegers["receive.message.max.bytes"] = 100 * MiB;
  Settings.ConfigurationIntegers["message.max.bytes"] = 100 * MiB;

  Settings.Address = Broker;
  Settings.PollTimeoutMS = 2000;

  ConnectThread = std::thread([&] {
    this->connect(std::ref(TopicName));
    return;
  });

  std::unique_lock<std::mutex> lk(ConnectionLock);
  ConnectionInit.wait(lk, [&] { return this->Initialising.load(); });
}

FileWriter::Streamer::~Streamer() { closeStream(); }

/// Create a RdKafka::Consumer a vector containing all the TopicPartition for
/// the given topic. If a start time is specified retrieve the correct initial
/// log. Assign the TopicPartition vector to the Consumer
/// \param TopicName the topic that the Streamer will consume
/// \param Options a StreamerOptions object
/// that contains configuration parameters for the Streamer and the KafkaConfig
void FileWriter::Streamer::connect(const std::string &TopicName) {
  std::lock_guard<std::mutex> lock(ConnectionReady);

  std::lock_guard<std::mutex> lk(ConnectionLock);
  Initialising = true;
  ConnectionInit.notify_all();

  LOG(Sev::Debug, "Connecting to {}", TopicName);
  ConsumerW.reset(new KafkaW::Consumer(Settings));
  ConsumerW->addTopic(TopicName);
  ////
  // auto Config = createConfiguration(Options);
  // if (!Config) {
  //   return;
  // }
  // RunStatus = createConsumer(std::move(Config));
  // if (RunStatus != SEC::no_error) {
  //   return;
  // }
  // auto Metadata = createMetadata();
  // if (!Metadata) {
  //   return;
  // }
  // LOG(Sev::Debug, "createMetadata");
  // RunStatus = createTopicPartition(TopicName, std::move(Metadata));
  // if (RunStatus != SEC::no_error) {
  //   return;
  // }
  // LOG(Sev::Debug, "createTopicPartition");
  // RunStatus = assignTopicPartition();
  // if (RunStatus != SEC::no_error) {
  //   return;
  // }
  LOG(Sev::Debug, "Connected to topic {}", TopicName);
  RunStatus = SEC::writing;
}

FileWriter::Streamer::SEC FileWriter::Streamer::closeStream() {
  std::lock_guard<std::mutex> lock(ConnectionReady);
  if (ConnectThread.joinable()) {
    ConnectThread.join();
  }
  if (Consumer) {
    Consumer->close();
  }
  TopicPartitionVector.clear();
  //  if (RunStatus == SEC::writing) {
  RunStatus = SEC::has_finished;
  //  }
  return RunStatus;
}

template <>
FileWriter::ProcessMessageResult
FileWriter::Streamer::write(FileWriter::DemuxTopic &MessageProcessor) {

  if (RunStatus == SEC::not_initialized) {
    return ProcessMessageResult::OK();
  }
  std::lock_guard<std::mutex> lock(
      ConnectionReady); // make sure that connect is completed

  if (int(RunStatus) < 0) {
    return ProcessMessageResult::ERR();
  }
  if (RunStatus == SEC::has_finished) {
    return ProcessMessageResult::STOP();
  }

  KafkaW::PollStatus Poll = ConsumerW->poll();
  LOG(Sev::Critical, "Poll isOk() : {}", Poll.isOk());
  LOG(Sev::Critical, "Poll isEOP() : {}", Poll.isEOP());
  LOG(Sev::Critical, "Poll isErr() : {}", Poll.isErr());
  LOG(Sev::Critical, "Poll isEmpty() : {}", Poll.isEmpty());

  if (Poll.isOk()) {
    Msg Message(Msg::fromKafkaW(std::move(Poll.isMsg())));

    if (Message.type != MsgType::Invalid) {
      auto MessageTime = MessageProcessor.time_difference_from_message(Message);
      LOG(Sev::Critical, "{} : {}", MessageTime.sourcename, MessageTime.dt);
    }
  }
  //////////////// Consumer.cxx
  // if (StartTime.count() > 0) {
  //   rd_kafka_offsets_for_times(RdKafka, PartitionList, 1000);
  //   rd_kafka_topic_partition_list_add(PartitionList, Topic.c_str(),
  // Partition)
  //       ->offset = StartTime.count();
  // } else {
  //
  //
  // // Consume the message and check for message errors. Timeout
  // // is considered ok;
  // // partition EOF return stop if the system time is larger than the stop
  // time;
  // // other errors are considered failure
  // std::unique_ptr<RdKafka::Message> msg(
  //     Consumer->consume(Options.ConsumerTimeout.count()));
  // if ((msg->err() == RdKafka::ERR__TIMED_OUT) ||
  //     (msg->err() == RdKafka::ERR__PARTITION_EOF)) {
  //   LOG(Sev::Debug, "consume :\t{}", RdKafka::err2str(msg->err()));
  //   if ((Options.StopTimestamp.count() > 0) &&
  //       (systemTime() > (Options.StopTimestamp + Options.AfterStopTime))) {
  //     Sources.clear();
  //     return ProcessMessageResult::STOP();
  //   }
  //   return ProcessMessageResult::OK();
  // }
  // if (msg->err() != RdKafka::ERR_NO_ERROR) {
  //   LOG(Sev::Warning, "Failed to consume :\t{}",
  // RdKafka::err2str(msg->err()));
  //   MessageInfo.error();
  //   return ProcessMessageResult::ERR();
  // }

  // size_t MessageLength = msg->len();
  // auto Message = Msg::rdkafka(std::move(msg));
  // // if the source is not in the source_list return OK (ignore)
  // // if StartTimestamp is set and timestamp < start_time skip message and
  // return
  // // OK, if StopTimestamp is set, timestamp > stop_time and the source is
  // still
  // // present remove source and return STOP else process the message
  // auto MessageTime = MessageProcessor.time_difference_from_message(Message);
  // LOG(Sev::Debug, "Source is {}", MessageTime.sourcename);
  // if (std::find(Sources.begin(), Sources.end(), MessageTime.sourcename) ==
  //     Sources.end()) {
  //   return ProcessMessageResult::OK();
  // }
  // if (MessageTime.dt < Options.StartTimestamp.count()) {
  //   return ProcessMessageResult::OK();
  // }
  // if (Options.StopTimestamp.count() > 0 &&
  //     MessageTime.dt > Options.StopTimestamp.count()) {
  //   if (removeSource(MessageTime.sourcename)) {
  //     return ProcessMessageResult::STOP();
  //   }
  //   return ProcessMessageResult::ERR();
  // }

  // // Collect information about the data received
  // MessageInfo.message(MessageLength);

  // // Write the message. Log any error and return the result of processing
  // auto result = MessageProcessor.process_message(std::move(Message));
  // LOG(Sev::Debug, "{} : Message timestamp : {}",
  //     TopicPartitionVector[0]->topic(), result.ts());
  // if (!result.is_OK()) {
  //   MessageInfo.error();
  // }
  // return result;
  return ProcessMessageResult::ERR();
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

/// Method that parse the json configuration and parse the options to be used in
/// RdKafka::Config
void
FileWriter::StreamerOptions::setRdKafkaOptions(const rapidjson::Value *Opt) {

  if (!Opt->IsObject()) {
    LOG(Sev::Warning, "Unable to parse steamer options");
    return;
  }

  for (auto &m : Opt->GetObject()) {
    if (m.value.IsString()) {
      RdKafkaOptions.emplace_back(m.name.GetString(), m.value.GetString());
      continue;
    }
    if (m.value.IsInt()) {
      RdKafkaOptions.emplace_back(m.name.GetString(),
                                  std::to_string(m.value.GetInt()));
      continue;
    }
  }
}

/// Method that parse the json configuration and sets the parameters used in the
/// Streamer
void
FileWriter::StreamerOptions::setStreamerOptions(const rapidjson::Value *Opt) {

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
