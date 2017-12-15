#include "Streamer.h"

#include "logger.h"
#include "helper.h"

#include <librdkafka/rdkafkacpp.h>

#include <memory>

/// Create and configure the RdKafka configuration used in the Streamer. If the
/// RdKafka fails in the creations of the configuration log the error, set
/// RunStatus to SEC::configuration_error and return an empty pointer. Else
/// return a unique_ptr pointing to the configuration. The configuration is
/// created using the options described in Options. For the list of available
/// options see
/// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md . If an
/// invalid option is passed log a warning.
/// \param Options an instance of StreamerOptions that contains the RdKafka::Conf options.
std::unique_ptr<RdKafka::Conf> FileWriter::Streamer::createConfiguration(
    const FileWriter::StreamerOptions& Options) {
  
  std::unique_ptr<RdKafka::Conf> Conf(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!Conf) {
    LOG(Sev::Error, "Error: invalid configuration");
    RunStatus = SEC::configuration_error;
    return nullptr;
  }

  std::string ErrStr;
  for(std::pair<std::string,std::string> Option : Options.RdKafkaOptions) {
    if (Conf->set(Option.first, Option.second, ErrStr) != RdKafka::Conf::CONF_OK) {
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
  RdKafka::Metadata *Metadata;
  std::unique_ptr<RdKafka::Topic> Topic;
  int retry{0};
  auto err = Consumer->metadata(Topic.get() != NULL, Topic.get(), &Metadata, 1000);
  while (err != RdKafka::ERR_NO_ERROR && retry < Options.NumMetadataRetry) {
    err = Consumer->metadata(Topic.get() != NULL, Topic.get(), &Metadata, 1000);
    ++retry;
  }
  if (err != RdKafka::ERR_NO_ERROR) {
      LOG(Sev::Error, "{}", RdKafka::err2str(err));
    RunStatus = SEC::metadata_error;
    return std::unique_ptr<RdKafka::Metadata>{nullptr};
  } else {
    return std::unique_ptr<RdKafka::Metadata>{Metadata};
  }
}

/// Reads the metadata structure and fills the RdKafka::TopicPartition vector with all the partitions corresponding to the given topic
/// \param metadata
FileWriter::Status::StreamerErrorCode
FileWriter::Streamer::createTopicPartition(
                                           const std::string &TopicName, std::unique_ptr<RdKafka::Metadata> &&Metadata) {
  if (RunStatus == SEC::metadata_error || !Metadata) {
    return SEC::metadata_error;
  }
  using PartitionMetadataVector =
      std::vector<const RdKafka::PartitionMetadata *>;
  const PartitionMetadataVector *pmv{nullptr};
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
  if (pmv->size()) {
    for (auto p : *pmv) {
      pushTopicPartition(TopicName, p->id());
      if (!TopicPartitionVector.back()) {
        LOG(Sev::Error, "Error: unable to create partition {} for topic {}", p->id(),
            TopicName);
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
    auto value = Options.StartTimestamp - Options.BeforeStartTime;
    TopicPartitionVector.push_back(
        RdKafka::TopicPartition::create(TopicName, Partition, value.count()));
  } else {
    TopicPartitionVector.push_back(RdKafka::TopicPartition::create(TopicName, Partition,
                                                  RdKafka::Topic::OFFSET_END));
  }
}

/// Assign the RdKafka::TopicPartition vector to the consumer at the correct point in time. If the Consumer is not allocated, the TopicPartitionVector empty (e.g. the topic is not found) or can't assign the consumer return SEC::topic_partition_error, else SEC::no_error
FileWriter::Streamer::SEC FileWriter::Streamer::assignTopicPartition() {
  if (!Consumer || TopicPartitionVector.empty()) {
    return SEC::topic_partition_error;
  }
  auto err = Consumer->offsetsForTimes(TopicPartitionVector, 1000);
  err = Consumer->assign(TopicPartitionVector);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Error, "{}", RdKafka::err2str(err));
    return SEC::topic_partition_error;
  }
  return SEC::no_error;
};

FileWriter::Streamer::Streamer(const std::string &Broker,
                               const std::string &TopicName,
                               const FileWriter::StreamerOptions& Opts)
  : RunStatus{SEC::not_initialized}, Options(Opts) {

  if (TopicName.empty() || Broker.empty()) {
    LOG(Sev::Error, "Broker and topic required");
    RunStatus = SEC::not_initialized;
    return;
  }
  Options.RdKafkaOptions.push_back({"metadata.broker.list",Broker});
  Options.RdKafkaOptions.push_back({"api.version.request", "true"});
  Options.RdKafkaOptions.push_back({"group.id","topic"});
  
  ConnectThread = std::thread([&] {
    this->connect(std::ref(TopicName));
    return;
  });

  std::unique_lock<std::mutex> lk(ConnectionLock);
  ConnectionInit.wait(lk, [&] { return this->Initilialising.load(); });
}

FileWriter::Streamer::~Streamer() { closeStream(); }

/// Create a RdKafka::Consumer a vector containing all the TopicPartition for
/// the given topic. If a start time is specified retrieve the correct initial
/// log. Assign the TopicPartition vector to the Consumer
/// \param TopicName the topic that the Streamer will consume
/// \param Options a StreamerOptions object
/// that contains configuration parameters for the Streamer and the KafkaConfig
void FileWriter::Streamer::connect(
    const std::string &TopicName) {
  std::lock_guard<std::mutex> lock(ConnectionReady);

  std::lock_guard<std::mutex> lk(ConnectionLock);
  Initilialising = true;
  ConnectionInit.notify_all();

  LOG(Sev::Debug, "Connecting to {}", TopicName);
  auto Config = createConfiguration(Options);
  if (!Config) {
    return;
  }
  RunStatus = createConsumer(std::move(Config));
  if (RunStatus != SEC::no_error) {
    return;
  }
  auto Metadata = createMetadata();
  if (!Metadata) {
    return;
  }
  LOG(Sev::Debug,"createMetadata");
  RunStatus = createTopicPartition(TopicName, std::move(Metadata));
  if (RunStatus != SEC::no_error) {
    return;
  }
  LOG(Sev::Debug,"createTopicPartition");
  RunStatus = assignTopicPartition();
  if (RunStatus != SEC::no_error) {
    return;
  }
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
  if (RunStatus == SEC::writing) {
    RunStatus = SEC::has_finished;
  }
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
    return ProcessMessageResult::OK();
  }

  std::unique_ptr<RdKafka::Message> msg{
      Consumer->consume(Options.ConsumerTimeout.count())};

  if (msg->err() == RdKafka::ERR__PARTITION_EOF ||
      msg->err() == RdKafka::ERR__TIMED_OUT) {
    LOG(Sev::Debug, "consume :\t{}", RdKafka::err2str(msg->err()));
    return ProcessMessageResult::OK();
  }
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
      LOG(Sev::Warning, "Failed to consume :\t{}", RdKafka::err2str(msg->err()));
    MessageInfo.error();
    return ProcessMessageResult::ERR();
  }

  // skip message if timestamp < start_time
  auto td = MessageProcessor.time_difference_from_message(
      (char *)msg->payload(), msg->len());
  if (td.dt < Options.StartTimestamp.count()) {
    return ProcessMessageResult::OK();
  }

  MessageInfo.message(msg->len());

  auto result =
      MessageProcessor.process_message((char *)msg->payload(), msg->len());
  LOG(Sev::Debug, "{} : Message timestamp : {}", TopicPartitionVector[0]->topic(),
      result.ts());
  if (!result.is_OK()) {
    MessageInfo.error();
  }
  return result;
}

/// Method that parse the json configuration and parse the options to be used in
/// RdKafka::Config
void FileWriter::StreamerOptions::setRdKafkaOptions(
    const rapidjson::Value *Opt) {

  if(!Opt->IsObject()) {
    LOG(Sev::Warning,"Unable to parse steamer options");
    return;
  }
  
  for (auto& m : Opt->GetObject() ) {
    if(m.value.IsString()) {
      RdKafkaOptions.push_back({m.name.GetString(),
            m.value.GetString()});
      continue;
    }
    if(m.value.IsInt()) {
      RdKafkaOptions.push_back({m.name.GetString(),
            std::to_string(m.value.GetInt())});
      continue;
    }
  }
}

/// Method that parse the json configuration and sets the parameters used in the
/// Streamer
void FileWriter::StreamerOptions::setStreamerOptions(const rapidjson::Value *Opt) {

  if(!Opt->IsObject()) {
    LOG(Sev::Warning,"Unable to parse steamer options");
    return;
  }

  for (auto& m : Opt->GetObject() ) {
    if(m.name.IsString()) {
      if(strncmp(m.name.GetString(),"ms-before-start",15)==0) {
        if (m.value.IsInt()) {
          BeforeStartTime = milliseconds(m.value.GetInt());
          continue;
        }
        LOG(Sev::Warning,"{} : wrong format",m.name.GetString());
      }
      if(strncmp(m.name.GetString(),"consumer-timeout-ms",19)==0) {
        if (m.value.IsInt()) {
          ConsumerTimeout = milliseconds(m.value.GetInt());
          continue;
        }
        LOG(Sev::Warning,"{} : wrong format",m.name.GetString());
      }
      if(strncmp(m.name.GetString(),"metadata-retry",14)==0) {
        if (m.value.IsInt()) {
          NumMetadataRetry = m.value.GetInt();
          continue;
        }
        LOG(Sev::Warning,"{} : wrong format",m.name.GetString());
      }
      LOG(Sev::Warning,"Unknown option {}, ignore",m.name.GetString());
    }
  }
}
