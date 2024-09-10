// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Partition.h"
#include "Msg.h"

namespace Stream {

std::vector<std::unique_ptr<ISourceFilter>>
create_filters(SrcToDst const &map, time_point start_time, time_point stop_time,
               MessageWriter *writer, Metrics::IRegistrar *registrar) {
  std::map<FileWriter::FlatbufferMessage::SrcHash,
           std::unique_ptr<SourceFilter>>
      hash_to_filter;
  std::map<FileWriter::FlatbufferMessage::SrcHash,
           FileWriter::FlatbufferMessage::SrcHash>
      write_hash_to_source_hash;
  for (auto const &src_dest_info : map) {
    // Note that the cppcheck warning we are suppressing here is an actual
    // false positive due to side effects of instantiating the SourceFilter
    if (hash_to_filter.find(src_dest_info.WriteHash) == hash_to_filter.end()) {
      hash_to_filter.emplace(src_dest_info.WriteHash,
                             // cppcheck-suppress stlFindInsert
                             std::make_unique<SourceFilter>(
                                 start_time, stop_time,
                                 src_dest_info.AcceptsRepeatedTimestamps,
                                 writer,
                                 registrar->getNewRegistrar(
                                     src_dest_info.getMetricsNameString())));
    }
    hash_to_filter[src_dest_info.WriteHash]->add_writer_module_for_message(
        src_dest_info.Destination);
    write_hash_to_source_hash[src_dest_info.WriteHash] = src_dest_info.SrcHash;
  }
  std::vector<std::unique_ptr<ISourceFilter>> filters;
  for (auto &[hash, filter] : hash_to_filter) {
    auto UsedHash = write_hash_to_source_hash[hash];
    filter->set_source_hash(UsedHash);
    filters.emplace_back(std::move(filter));
  }
  return filters;
}

std::unique_ptr<Partition> Partition::create(
    std::shared_ptr<Kafka::ConsumerInterface> consumer, int partition,
    const std::string &topic_name, const SrcToDst &map, MessageWriter *writer,
    Metrics::IRegistrar *registrar, time_point start_time, time_point stop_time,
    duration stop_leeway, duration kafka_error_timeout,
    const std::function<bool()> &streamers_paused_function) {
  auto filters = create_filters(map, start_time, stop_time, writer, registrar);
  auto partition_filter = std::make_unique<PartitionFilter>(
      stop_time, stop_leeway, kafka_error_timeout);
  return std::make_unique<Partition>(
      std::move(consumer), partition, topic_name, std::move(filters),
      std::move(partition_filter), registrar, stop_time, stop_leeway,
      streamers_paused_function);
}

Partition::Partition(std::shared_ptr<Kafka::ConsumerInterface> consumer,
                     int partition, std::string const &topic_name,
                     std::vector<std::unique_ptr<ISourceFilter>> source_filters,
                     std::unique_ptr<PartitionFilter> partition_filter,
                     Metrics::IRegistrar *registrar, time_point stop_time,
                     duration stop_leeway,
                     std::function<bool()> const &streamers_paused_function)
    : _consumer(std::move(consumer)), _partition_id(partition),
      _topic_name(topic_name), _stop_time(stop_time),
      _stop_time_leeway(stop_leeway),
      _partition_filter(std::move(partition_filter)),
      _source_filters(std::move(source_filters)),
      _streamers_paused_function(streamers_paused_function) {
  _stop_time = sanitise_stop_time(stop_time);
  _partition_filter->setStopTime(_stop_time);

  registrar->registerMetric(KafkaTimeouts, {Metrics::LogTo::CARBON});
  registrar->registerMetric(KafkaErrors,
                            {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  registrar->registerMetric(EndOfPartition, {Metrics::LogTo::CARBON});
  registrar->registerMetric(MessagesReceived, {Metrics::LogTo::CARBON});
  registrar->registerMetric(MessagesProcessed, {Metrics::LogTo::CARBON});
  registrar->registerMetric(BadOffsets,
                            {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  registrar->registerMetric(FlatbufferErrors,
                            {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  registrar->registerMetric(BadFlatbufferTimestampErrors,
                            {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  registrar->registerMetric(UnknownFlatbufferIdErrors,
                            {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  registrar->registerMetric(NotValidFlatbufferErrors,
                            {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  registrar->registerMetric(BufferTooSmallErrors,
                            {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
}

// Old constructor - to be removed
Partition::Partition(std::shared_ptr<Kafka::ConsumerInterface> consumer,
                     int partition, std::string const &topic_name,
                     SrcToDst const &map, MessageWriter *writer,
                     Metrics::IRegistrar *registrar, time_point start_time,
                     time_point stop_time, duration stop_leeway,
                     duration kafka_error_timeout,
                     std::function<bool()> const &streamers_paused_function)
    : Partition(std::move(consumer), partition, topic_name,
                create_filters(map, start_time, stop_time, writer, registrar),
                std::make_unique<PartitionFilter>(stop_time, stop_leeway,
                                                  kafka_error_timeout),
                registrar, stop_time, stop_leeway, streamers_paused_function) {}

void Partition::forceStop() { _force_stop = true; }

void Partition::sleep(const duration Duration) const {
  std::this_thread::sleep_for(Duration);
}

void Partition::stop() { forceStop(); }

time_point Partition::sanitise_stop_time(time_point stop_time) {
  // Stop time is reduced if it is too close to max to avoid overflow.
  if (time_point::max() - stop_time <= _stop_time_leeway) {
    stop_time -= _stop_time_leeway;
  }
  return stop_time;
}

void Partition::setStopTime(time_point Stop) {
  _stop_time = sanitise_stop_time(Stop);
  _partition_filter->setStopTime(_stop_time);
  for (auto &Filter : _source_filters) {
    Filter->set_stop_time(_stop_time);
  }
}

bool Partition::hasFinished() const { return _has_finished.load(); }

void Partition::checkAndLogPartitionTimeOut() {
  if (_partition_filter->hasTopicTimedOut()) {
    if (!_partition_time_out_logged) {
      Logger::Info(
          "No new messages were received from Kafka in partition {} of "
          "topic {} ({:.1f}s passed) when polling for new data.",
          _partition_id, _topic_name,
          double(std::chrono::duration_cast<std::chrono::milliseconds>(
                     system_clock::now() -
                     _partition_filter->getStatusOccurrenceTime())
                     .count()) /
              1000.0);
      _partition_time_out_logged = true;
    }
  } else {
    _partition_time_out_logged = false;
  }
}

bool Partition::hasStopBeenRequested() const { return _force_stop; }

bool Partition::shouldStopBasedOnPollStatus(Kafka::PollStatus CStatus) {
  checkAndLogPartitionTimeOut();
  if (_partition_filter->shouldStopPartition(CStatus)) {
    switch (_partition_filter->currentPartitionState()) {
    case PartitionFilter::PartitionState::ERROR:
      Logger::Error(
          "Stopping consumption of data from Kafka in partition {} of "
          "topic {} due to poll error.",
          _partition_id, _topic_name);
      break;
    case PartitionFilter::PartitionState::END_OF_PARTITION:
      Logger::Info(
          R"(Done consuming data from partition {} of topic "{}" (reached the end of the partition).)",
          _partition_id, _topic_name);
      break;
    case PartitionFilter::PartitionState::TIMEOUT:
    case PartitionFilter::PartitionState::DEFAULT:
    default:
      Logger::Info(R"(Done consuming data from partition {} of topic "{}".)",
                   _partition_id, _topic_name);
    }
    return true;
  }
  return false;
}

void Partition::pollForMessage() {
  if (_force_stop) {
    _has_finished = true;
    return;
  }
  if (_streamers_paused_function()) {
    sleep(_pause_check_interval);
  } else {
    auto Msg = _consumer->poll();
    switch (Msg.first) {
    case Kafka::PollStatus::Message:
      MessagesReceived++;
      break;
    case Kafka::PollStatus::EndOfPartition:
      EndOfPartition++;
      break;
    case Kafka::PollStatus::TimedOut:
      KafkaTimeouts++;
      break;
    case Kafka::PollStatus::Error:
      KafkaErrors++;
      break;
    default:
      // Do nothing
      break;
    }
    if (shouldStopBasedOnPollStatus(Msg.first)) {
      _has_finished = true;
      return;
    }

    if (Msg.first == Kafka::PollStatus::Message) {
      processMessage(Msg.second);
      if (_source_filters.empty()) {
        Logger::Info(
            R"(Done consuming data from partition {} of topic "{}" as there are no remaining filters.)",
            _partition_id, _topic_name);
        _has_finished = true;
        return;
      } else if (Msg.second.getMetaData().timestamp() >
                 _stop_time + _stop_time_leeway) {
        Logger::Info(
            R"(Done consuming data from partition {} of topic "{}" as we have reached the stop time. The timestamp of the last message was: {})",
            _partition_id, _topic_name, Msg.second.getMetaData().timestamp());
        _has_finished = true;
        return;
      }
    }
  }
}

void Partition::processMessage(FileWriter::Msg const &Message) {
  if (_current_offset != 0 &&
      _current_offset + 1 != Message.getMetaData().Offset) {
    BadOffsets++;
  }
  _current_offset = Message.getMetaData().Offset;
  FileWriter::FlatbufferMessage FbMsg;
  try {
    FbMsg = FileWriter::FlatbufferMessage(Message);
  } catch (FileWriter::BufferTooSmallError &) {
    BufferTooSmallErrors++;
    FlatbufferErrors++;
    return;
  } catch (FileWriter::InvalidFlatbufferTimestamp &) {
    BadFlatbufferTimestampErrors++;
    FlatbufferErrors++;
    return;
  } catch (FileWriter::UnknownFlatbufferID &) {
    UnknownFlatbufferIdErrors++;
    FlatbufferErrors++;
    return;
  } catch (FileWriter::NotValidFlatbuffer &) {
    NotValidFlatbufferErrors++;
    FlatbufferErrors++;
    return;
  } catch (std::exception &) {
    FlatbufferErrors++;
    return;
  }

  bool processed = false;
  for (auto const &filter : _source_filters) {
    if (filter->filter_message(FbMsg)) {
      processed = true;
    }
  }

  if (processed) {
    MessagesProcessed++;
  }

  _source_filters.erase(
      std::remove_if(_source_filters.begin(), _source_filters.end(),
                     [](auto &filter) { return filter->has_finished(); }),
      _source_filters.end());
}

} // namespace Stream
