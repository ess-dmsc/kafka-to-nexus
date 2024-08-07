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

std::vector<std::pair<FileWriter::FlatbufferMessage::SrcHash,
                      std::unique_ptr<SourceFilter>>>
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
  std::vector<std::pair<FileWriter::FlatbufferMessage::SrcHash,
                        std::unique_ptr<SourceFilter>>>
      filters;
  for (auto &[hash, filter] : hash_to_filter) {
    auto UsedHash = write_hash_to_source_hash[hash];
    filters.emplace_back(UsedHash, std::move(filter));
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
  return std::make_unique<Partition>(
      std::move(consumer), partition, topic_name, std::move(filters), registrar,
      stop_time, stop_leeway, kafka_error_timeout, streamers_paused_function);
}

Partition::Partition(
    std::shared_ptr<Kafka::ConsumerInterface> consumer, int partition,
    std::string const &topic_name,
    std::vector<std::pair<FileWriter::FlatbufferMessage::SrcHash,
                          std::unique_ptr<SourceFilter>>>
        source_filters,
    Metrics::IRegistrar *registrar, time_point stop_time, duration stop_leeway,
    duration kafka_error_timeout,
    std::function<bool()> const &streamers_paused_function)
    : ConsumerPtr(std::move(consumer)), PartitionID(partition),
      Topic(topic_name), StopTime(stop_time), StopTimeLeeway(stop_leeway),
      StopTester(stop_time, stop_leeway, kafka_error_timeout),
      _source_filters(std::move(source_filters)),
      AreStreamersPausedFunction(streamers_paused_function) {
  // Stop time is reduced if it is too close to max to avoid overflow.
  if (time_point::max() - StopTime <= StopTimeLeeway) {
    StopTime -= StopTimeLeeway;
  }

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

Partition::Partition(std::shared_ptr<Kafka::ConsumerInterface> consumer,
                     int partition, std::string const &topic_name,
                     SrcToDst const &map, MessageWriter *writer,
                     Metrics::IRegistrar *registrar, time_point start_time,
                     time_point stop_time, duration stop_leeway,
                     duration kafka_error_timeout,
                     std::function<bool()> const &streamers_paused_function)
    : Partition(std::move(consumer), partition, topic_name,
                create_filters(map, start_time, stop_time, writer, registrar),
                registrar, stop_time, stop_leeway, kafka_error_timeout,
                streamers_paused_function) {}

void Partition::start() { addPollTask(); }

void Partition::forceStop() { StopTester.forceStop(); }

void Partition::sleep(const duration Duration) const {
  std::this_thread::sleep_for(Duration);
}

void Partition::stop() {
  Executor.sendLowPriorityWork([=]() { forceStop(); });
  Executor.sendWork([=]() { forceStop(); });
}

void Partition::setStopTime(time_point Stop) {
  Executor.sendWork([=]() {
    StopTime = Stop;
    StopTester.setStopTime(Stop);
    for (auto &Filter : _source_filters) {
      Filter.second->set_stop_time(Stop);
    }
  });
}

bool Partition::hasFinished() const { return HasFinished.load(); }

void Partition::addPollTask() {
  Executor.sendLowPriorityWork([=]() { pollForMessage(); });
}

void Partition::checkAndLogPartitionTimeOut() {
  if (StopTester.hasTopicTimedOut()) {
    if (!PartitionTimeOutLogged) {
      Logger::Info(
          "No new messages were received from Kafka in partition {} of "
          "topic {} ({:.1f}s passed) when polling for new data.",
          PartitionID, Topic,
          double(std::chrono::duration_cast<std::chrono::milliseconds>(
                     system_clock::now() - StopTester.getStatusOccurrenceTime())
                     .count()) /
              1000.0);
      PartitionTimeOutLogged = true;
    }
  } else {
    PartitionTimeOutLogged = false;
  }
}

bool Partition::hasStopBeenRequested() const {
  return StopTester.hasForceStopBeenRequested();
}

bool Partition::shouldStopBasedOnPollStatus(Kafka::PollStatus CStatus) {
  checkAndLogPartitionTimeOut();
  if (StopTester.shouldStopPartition(CStatus)) {
    switch (StopTester.currentPartitionState()) {
    case PartitionFilter::PartitionState::ERROR:
      Logger::Error(
          "Stopping consumption of data from Kafka in partition {} of "
          "topic {} due to poll error.",
          PartitionID, Topic);
      break;
    case PartitionFilter::PartitionState::END_OF_PARTITION:
      Logger::Info(
          R"(Done consuming data from partition {} of topic "{}" (reached the end of the partition).)",
          PartitionID, Topic);
      break;
    case PartitionFilter::PartitionState::TIMEOUT:
    case PartitionFilter::PartitionState::DEFAULT:
    default:
      Logger::Info(R"(Done consuming data from partition {} of topic "{}".)",
                   PartitionID, Topic);
    }
    return true;
  }
  return false;
}

void Partition::pollForMessage() {
  if (hasStopBeenRequested()) {
    HasFinished = true;
    return;
  }
  if (AreStreamersPausedFunction()) {
    sleep(PauseCheckInterval);
  } else {
    auto Msg = ConsumerPtr->poll();
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
      HasFinished = true;
      return;
    }

    if (Msg.first == Kafka::PollStatus::Message) {
      processMessage(Msg.second);
      if (_source_filters.empty()) {
        Logger::Info(
            R"(Done consuming data from partition {} of topic "{}" as there are no remaining filters.)",
            PartitionID, Topic);
        HasFinished = true;
        return;
      } else if (Msg.second.getMetaData().timestamp() >
                 StopTime + StopTimeLeeway) {
        Logger::Info(
            R"(Done consuming data from partition {} of topic "{}" as we have reached the stop time. The timestamp of the last message was: {})",
            PartitionID, Topic, Msg.second.getMetaData().timestamp());
        HasFinished = true;
        return;
      }
    }
  }
  addPollTask();
}

void Partition::processMessage(FileWriter::Msg const &Message) {
  if (CurrentOffset != 0 && CurrentOffset + 1 != Message.getMetaData().Offset) {
    BadOffsets++;
  }
  CurrentOffset = Message.getMetaData().Offset;
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
  for (auto &[hash, filter] : _source_filters) {
    if (hash == FbMsg.getSourceHash()) {
      processed = true;
      filter->filter_message(FbMsg);
    }
  }

  if (processed) {
    MessagesProcessed++;
  }

  _source_filters.erase(
      std::remove_if(_source_filters.begin(), _source_filters.end(),
                     [](auto &Item) { return Item.second->has_finished(); }),
      _source_filters.end());
}

} // namespace Stream
