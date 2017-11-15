#include <algorithm>
#include <memory>

#include "logger.h"
#include <librdkafka/rdkafkacpp.h>

#include "Msg.h"
#include "Streamer.hpp"
#include "helper.h"
#include <unistd.h>

bool FileWriter::Streamer::set_streamer_opt(
    const FileWriter::Streamer::option_t &option) {
  // Note: to_int<> returns a pair (validity of conversion,value).
  // First element notifies if the conversion is defined

  if (option.first == "ms.before.start") {
    auto value = to_num<int>(option.second);
    if (value.first && (value.second > 0)) {
      ms_before_start_time = ESSTimeStamp(value.second);
      return true;
    }
  }
  if (option.first == "streamer.metadata.retry") {
    auto value = to_num<int>(option.second);
    if (value.first && (value.second > 0)) {
      metadata_retry = value.second;
      return true;
    }
  }
  return false;
}

bool FileWriter::Streamer::set_conf_opt(
    std::shared_ptr<RdKafka::Conf> conf,
    const FileWriter::Streamer::option_t &option) {
  std::string errstr;
  if (!(option.first.empty() || option.second.empty())) {
    auto result = conf->set(option.first, option.second, errstr);
    LOG(Sev::Debug, "set kafka config: {} = {}", option.first, option.second);
    if (result != RdKafka::Conf::CONF_OK) {
      LOG(Sev::Error, "Failed to initialise configuration: {}", errstr);
      return false;
    }
  }
  return true;
}

std::unique_ptr<RdKafka::Metadata>
FileWriter::Streamer::get_metadata(const int &retry) {
  RdKafka::Metadata *md;
  std::unique_ptr<RdKafka::Metadata> metadata{nullptr};
  std::unique_ptr<RdKafka::Topic> ptopic;
  auto err = _consumer->metadata(ptopic.get() != NULL, ptopic.get(), &md, 1000);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Error, "Can't request metadata");
    if (retry) {
      get_metadata(retry - 1);
    } else {
      return metadata;
    }
  } else {
    metadata.reset(md);
  }
  return metadata;
}

FileWriter::Streamer::SEC FileWriter::Streamer::get_topic_partitions(
    const std::string &topic, std::unique_ptr<RdKafka::Metadata> _metadata) {
  bool topic_found = false;
  if (!_metadata) {
    LOG(Sev::Error, "Missing metadata informations");
    return SEC::metadata_error;
  }
  auto partition_metadata = _metadata->topics()->at(0)->partitions();
  for (auto &i : *_metadata->topics()) {
    if (i->topic() == topic) {
      partition_metadata = i->partitions();
      topic_found = true;
    }
  }
  if (!topic_found) {
    LOG(Sev::Error, "Can't find topic : {}", topic);
    return SEC::topic_error;
  }
  for (auto &i : (*partition_metadata)) {
    _tp.push_back(RdKafka::TopicPartition::create(topic, i->id()));
  }
  return SEC::no_error;
}

FileWriter::Streamer::SEC FileWriter::Streamer::get_offset_boundaries() {
  for (auto &i : _tp) {
    int64_t high, low;
    auto err = _consumer->query_watermark_offsets(i->topic(), i->partition(),
                                                  &low, &high, 5000);
    if (err) {
      LOG(Sev::Error,
          "Unable to get boundaries for topic {}, partition {} : {}",
          i->topic(), i->partition(), RdKafka::err2str(err));
      return SEC::topic_partition_error;
    }
    _low.push_back(RdKafkaOffset(low));
  }
  return SEC::no_error;
}

std::shared_ptr<RdKafka::Conf> FileWriter::Streamer::initialize_configuration(
    FileWriter::Streamer::Options &kafka_options) {

  std::shared_ptr<RdKafka::Conf> conf(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  kafka_options.erase(std::remove_if(kafka_options.begin(), kafka_options.end(),
                                     [&](option_t &item) -> bool {
                                       return this->set_conf_opt(conf, item);
                                     }),
                      kafka_options.end());
  if (!kafka_options.empty()) {
    for (auto &item : kafka_options) {
      LOG(Sev::Warning, "Unknown option: {} [{}]", item.first, item.second);
    }
  }
  return conf;
}

void FileWriter::Streamer::initialize_streamer(
    FileWriter::Streamer::Options &filewriter_options) {
  filewriter_options.erase(std::remove_if(filewriter_options.begin(),
                                          filewriter_options.end(),
                                          [&](option_t &item) -> bool {
                                            return this->set_streamer_opt(item);
                                          }),
                           filewriter_options.end());
  if (!filewriter_options.empty()) {
    for (auto &item : filewriter_options) {
      LOG(Sev::Warning, "Unknown option: {} [{}]", item.first, item.second);
    }
  }
}

FileWriter::Streamer::Streamer(const std::string &broker,
                               const std::string &topic_name,
                               FileWriter::Streamer::Options kafka_options,
                               FileWriter::Streamer::Options filewriter_options)
    : run_status_{SEC::not_initialized} {

  if (topic_name.empty() || broker.empty()) {
    LOG(Sev::Warning, "Broker and topic required");
    run_status_ = SEC::not_initialized;
    return;
  }
  kafka_options.push_back(
      FileWriter::Streamer::option_t{"metadata.broker.list", broker});
  kafka_options.push_back(
      FileWriter::Streamer::option_t{"api.version.request", "true"});
  kafka_options.push_back(
      FileWriter::Streamer::option_t{"group.id", topic_name});

  connect_ = std::thread([&] {
    this->connect(topic_name, kafka_options, filewriter_options);
    return;
  });

  std::unique_lock<std::mutex> lk(connection_lock_);
  connection_init_.wait(lk, [&] { return this->initilialising_.load(); });
}

FileWriter::Streamer::~Streamer() { close_stream(); }

void FileWriter::Streamer::connect(
    const std::string topic_name, FileWriter::Streamer::Options kafka_options,
    FileWriter::Streamer::Options filewriter_options) {
  std::lock_guard<std::mutex> lock(connection_ready_);

  std::lock_guard<std::mutex> lk(connection_lock_);
  initilialising_ = true;
  connection_init_.notify_all();

  LOG(Sev::Info, "Connecting to {}", topic_name);
  for (auto &i : filewriter_options) {
    LOG(Sev::Debug, "{} :\t{}", i.first, i.second);
  }
  for (auto &i : kafka_options) {
    LOG(Sev::Debug, "{} :\t{}", i.first, i.second);
  }

  // set streamer options
  initialize_streamer(filewriter_options);

  std::string errstr;
  // Initialize configuration and consumer
  auto conf = initialize_configuration(kafka_options);
  if (!conf) {
    LOG(Sev::Error, "Unable to initialize configuration ({})", topic_name);
    run_status_ = SEC::configuration_error;
    return;
  }
  RdKafka::KafkaConsumer *c =
      RdKafka::KafkaConsumer::create(conf.get(), errstr);
  _consumer.reset(c);
  if (!_consumer) {
    LOG(Sev::Error, "Failed to create consumer: {}", errstr);
    run_status_ = SEC::consumer_error;
    return;
  }

  // Retrieve informations
  std::unique_ptr<RdKafka::Metadata> metadata = get_metadata(metadata_retry);
  if (!metadata) {
    LOG(Sev::Error, "Unable to retrieve metadata");
    run_status_ = SEC::metadata_error;
    return;
  }
  if (get_topic_partitions(topic_name, std::move(metadata)) != SEC::no_error) {
    LOG(Sev::Error, "Unable to build TopicPartitions structure");
    run_status_ = SEC::topic_partition_error;
    return;
  }
  // Assign consumer
  for (auto &i : _tp) {
    i->set_offset(_offset.value());
  }
  auto err = _consumer->assign(_tp);
  if (err) {
    LOG(Sev::Warning, "Failed to subscribe to {} ", topic_name);
    run_status_ = SEC::assign_error;
    return;
  }
  auto val = get_offset_boundaries();
  if (val != SEC::no_error) {
    run_status_ = val;
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
  if (_consumer) {
    _consumer->close();
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
      _consumer->consume(consumer_timeout.count())};

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
  // to be removed
  message_info_.message(msg->len());
  _offset = RdKafkaOffset(msg->offset());

  auto result = mp.process_message(Msg::rdkafka(std::move(msg)));
  LOG(Sev::Debug, "{} : Message timestamp : {}", _tp[0]->topic(), result.ts());
  if (!result.is_OK()) {
    message_info_.error();
  }
  return result;
}

FileWriter::Streamer::SEC
FileWriter::Streamer::set_start_time(const ESSTimeStamp &timepoint) {
  std::lock_guard<std::mutex> lock(
      connection_ready_); // make sure connnection is done

  auto value = std::chrono::duration_cast<KafkaTimeStamp>(timepoint -
                                                          ms_before_start_time);
  for (auto &i : _tp) {
    i->set_offset(value.count());
  }
  auto err = _consumer->offsetsForTimes(_tp, 1000);
  if (err != RdKafka::ERR_NO_ERROR) {
    run_status_ = SEC::start_time_error;
    LOG(Sev::Error, "Error searching initial time: {}", err2str(err));
    for (auto &i : _tp) {
      LOG(Sev::Error, "TopicPartition {}-{} : {}", i->topic(), i->partition(),
          RdKafka::err2str(i->err()));
    }
    run_status_ = SEC::start_time_error;
    return SEC::start_time_error;
  }
  if (err == RdKafka::ERR_NO_ERROR) {
    for (auto &i : _tp) {
      auto offset = i->offset();
      i->set_offset(offset);
    }
  }
  err = _consumer->assign(_tp);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Error, "Error assigning initial time: {}", err2str(err));
    run_status_ = SEC::start_time_error;
    return SEC::start_time_error;
  }
  return SEC::no_error;
}
