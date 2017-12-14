#include "Streamer.hpp"

#include "logger.h"
#include "helper.h"

#include <librdkafka/rdkafkacpp.h>

#include <algorithm>
#include <memory>
#include <unistd.h>

/// Method that creates and configures the RdKafka configuration used in the
/// Streamer
std::unique_ptr<RdKafka::Conf> FileWriter::Streamer::create_configuration(
    const FileWriter::StreamerOptions& Options) {
  
  std::unique_ptr<RdKafka::Conf> conf(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!conf) {
    LOG(Sev::Error, "Error: invalid configuration");
    run_status_ = SEC::configuration_error;
    return nullptr;
  }
  
  std::string errstr;
  for(std::pair<std::string,std::string> Option : Options.RdKafkaOptions) {
    if (conf->set(Option.first, Option.second, errstr) != RdKafka::Conf::CONF_OK) {
      LOG(Sev::Warning, "{}", errstr);
    }
  }
  return conf;
}

FileWriter::Status::StreamerErrorCode
FileWriter::Streamer::create_consumer(std::unique_ptr<RdKafka::Conf> &&conf) {
  std::string errstr;
  consumer.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
  if (!consumer) {
    LOG(Sev::Debug, "{}", errstr);
    return SEC::consumer_error;
  }
  return SEC::no_error;
}

std::unique_ptr<RdKafka::Metadata> FileWriter::Streamer::create_metadata() {
  RdKafka::Metadata *md;
  std::unique_ptr<RdKafka::Topic> ptopic;
  int retry{0};
  auto err = consumer->metadata(ptopic.get() != NULL, ptopic.get(), &md, 1000);
  while (err != RdKafka::ERR_NO_ERROR && retry < Options.NumMetadataRetry) {
    err = consumer->metadata(ptopic.get() != NULL, ptopic.get(), &md, 1000);
    ++retry;
  }
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Error, "{}", RdKafka::err2str(err));
    run_status_ = SEC::metadata_error;
    return std::unique_ptr<RdKafka::Metadata>{nullptr};
  } else {
    return std::unique_ptr<RdKafka::Metadata>{md};
  }
}

FileWriter::Status::StreamerErrorCode
FileWriter::Streamer::create_topic_partition(
    const std::string &topic, std::unique_ptr<RdKafka::Metadata> &&metadata) {
  if (run_status_ == SEC::metadata_error || !metadata) {
    return SEC::metadata_error;
  }
  using PartitionMetadataVector =
      std::vector<const RdKafka::PartitionMetadata *>;
  const PartitionMetadataVector *pmv{nullptr};
  for (auto &t : *metadata->topics()) {
    if (t->topic() == topic) {
      pmv = t->partitions();
      break;
    }
  }
  if (!pmv) {
    LOG(Sev::Error, "Error: unable to find partition for topic {}", topic);
    return SEC::topic_partition_error;
  }
  if (pmv->size()) {
    for (auto p : *pmv) {
      push_topic_partition(topic, p->id());
      if (!_tp.back()) {
        LOG(Sev::Error, "Error: unable to create partition {} for topic {}", p->id(),
            topic);
        return SEC::topic_partition_error;
      }
    }
  } else {
    LOG(Sev::Error, "Error: no partitions for topic {}", topic);
    return SEC::topic_partition_error;
  }

  return SEC::no_error;
}

void FileWriter::Streamer::push_topic_partition(const std::string &topic,
                                                const int32_t &partition) {
  if (Options.StartTimestamp.count()) {
    auto value = Options.StartTimestamp - Options.BeforeStartTime;
    _tp.push_back(
        RdKafka::TopicPartition::create(topic, partition, value.count()));
  } else {
    _tp.push_back(RdKafka::TopicPartition::create(topic, partition,
                                                  RdKafka::Topic::OFFSET_END));
  }
}

FileWriter::Status::StreamerErrorCode
FileWriter::Streamer::assign_topic_partition() {
  if (!consumer || _tp.empty()) {
    return SEC::topic_partition_error;
  }
  auto err = consumer->offsetsForTimes(_tp, 1000);
  err = consumer->assign(_tp);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Error, "{}", RdKafka::err2str(err));
    return SEC::topic_partition_error;
  }
  return SEC::no_error;
};

FileWriter::Streamer::Streamer(const std::string &broker,
                               const std::string &topic_name,
                               const FileWriter::StreamerOptions& Opts)
  : run_status_{SEC::not_initialized}, Options(Opts) {

  if (topic_name.empty() || broker.empty()) {
    LOG(Sev::Warning, "Broker and topic required");
    run_status_ = SEC::not_initialized;
    return;
  }
  Options.RdKafkaOptions.push_back({"metadata.broker.list",broker});
  Options.RdKafkaOptions.push_back({"api.version.request", "true"});
  Options.RdKafkaOptions.push_back({"group.id","topic"});
  
  connect_ = std::thread([&] {
    this->connect(std::ref(topic_name), std::ref(Options));
    return;
  });

  std::unique_lock<std::mutex> lk(connection_lock_);
  connection_init_.wait(lk, [&] { return this->initilialising_.load(); });
}

FileWriter::Streamer::~Streamer() { close_stream(); }

void FileWriter::Streamer::connect(
    const std::string &topic_name,
    const FileWriter::StreamerOptions& options) {
  std::lock_guard<std::mutex> lock(connection_ready_);

  std::lock_guard<std::mutex> lk(connection_lock_);
  initilialising_ = true;
  connection_init_.notify_all();

  LOG(Sev::Debug, "Connecting to {}", topic_name);

  auto config = create_configuration(options);
  if (!config) {
    return;
  }
  run_status_ = create_consumer(std::move(config));
  if (run_status_ != SEC::no_error) {
    return;
  }
  auto metadata = create_metadata();
  if (!metadata) {
    return;
  }
  LOG(Sev::Debug,"create_metadata");
  run_status_ = create_topic_partition(topic_name, std::move(metadata));
  if (run_status_ != SEC::no_error) {
    return;
  }
  LOG(Sev::Debug,"create_topic_partition");
  run_status_ = assign_topic_partition();
  if (run_status_ != SEC::no_error) {
    return;
  }
  LOG(Sev::Debug, "Connected to topic {}", topic_name);
  run_status_ = SEC::writing;
}

FileWriter::Streamer::SEC FileWriter::Streamer::close_stream() {
  std::lock_guard<std::mutex> lock(connection_ready_);
  if (connect_.joinable()) {
    connect_.join();
  }
  if (consumer) {
    consumer->close();
  }
  _tp.clear();
  if (run_status_ == SEC::writing) {
    run_status_ = SEC::has_finished;
  }
  return run_status_;
}

template <>
FileWriter::ProcessMessageResult
FileWriter::Streamer::write(FileWriter::DemuxTopic &mp) {

  if (run_status_ == SEC::not_initialized) {
    return ProcessMessageResult::OK();
  }
  std::lock_guard<std::mutex> lock(
      connection_ready_); // make sure that connect is completed

  if (int(run_status_) < 0) {
    return ProcessMessageResult::ERR();
  }
  if (run_status_ == SEC::has_finished) {
    return ProcessMessageResult::OK();
  }

  std::unique_ptr<RdKafka::Message> msg{
      consumer->consume(Options.ConsumerTimeout.count())};

  if (msg->err() == RdKafka::ERR__PARTITION_EOF ||
      msg->err() == RdKafka::ERR__TIMED_OUT) {
    LOG(Sev::Debug, "consume :\t{}", RdKafka::err2str(msg->err()));
    return ProcessMessageResult::OK();
  }
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Warning, "Failed to consume :\t{}", RdKafka::err2str(msg->err()));
    message_info_.error();
    return ProcessMessageResult::ERR();
  }

  // skip message if timestamp < start_time
  auto td = mp.time_difference_from_message((char *)msg->payload(), msg->len());
  if (td.dt < Options.StartTimestamp.count()) {
    return ProcessMessageResult::OK();
  }

  message_info_.message(msg->len());

  auto result = mp.process_message(Msg::rdkafka(std::move(msg)));
  LOG(Sev::Debug, "{} : Message timestamp : {}", _tp[0]->topic(), result.ts());
  if (!result.is_OK()) {
    message_info_.error();
  }
  return result;
}

/// Method that parse the json configuration and parse the options to be used in
/// RdKafka::Config
void FileWriter::StreamerOptions::SetRdKafkaOptions(
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
void FileWriter::StreamerOptions::SetStreamerOptions(const rapidjson::Value *Opt) {

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
