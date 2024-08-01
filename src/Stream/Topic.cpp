// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Topic.h"
#include "Kafka/BrokerSettings.h"
#include "Kafka/ConsumerFactory.h"
#include "Kafka/MetaDataQuery.h"
#include "logger.h"
#include <Kafka/MetadataException.h>

#include <iostream>
#include <utility>

namespace Stream {

Topic::Topic(Kafka::BrokerSettings const &Settings, std::string const &Topic,
             SrcToDst Map, MessageWriter *Writer,
             Metrics::IRegistrar *RegisterMetric, time_point StartTime,
             duration StartTimeLeeway, time_point StopTime,
             duration StopTimeLeeway,
             std::function<bool()> AreStreamersPausedFunction,
             std::shared_ptr<Kafka::MetadataEnquirer> metadata_enquirer,
             std::shared_ptr<Kafka::ConsumerFactoryInterface> consumer_factory)
    : KafkaSettings(Settings), TopicName(Topic), DataMap(std::move(Map)),
      WriterPtr(Writer), StartConsumeTime(StartTime),
      StartLeeway(StartTimeLeeway), StopConsumeTime(StopTime),
      StopLeeway(StopTimeLeeway),
      CurrentMetadataTimeOut(Settings.MinMetadataTimeout),
      Registrar(RegisterMetric->getNewRegistrar(Topic)),
      AreStreamersPausedFunction(std::move(AreStreamersPausedFunction)),
      _metadata_enquirer(std::move(metadata_enquirer)),
      _consumer_factory(std::move(consumer_factory)) {}

void Topic::start() {
  Executor.sendWork([=]() { initMetadataCalls(KafkaSettings, TopicName); });
}

void Topic::initMetadataCalls(Kafka::BrokerSettings const &Settings,
                              std::string const &Topic) {
  Executor.sendWork([=]() {
    CurrentMetadataTimeOut = Settings.MinMetadataTimeout;
    getPartitionsForTopic(Settings, Topic);
  });
}

void Topic::stop() {
  for (auto &Stream : ConsumerThreads) {
    Stream->stop();
  }
}

void Topic::setStopTime(std::chrono::system_clock::time_point StopTime) {
  Executor.sendWork([=]() {
    StopConsumeTime = StopTime;
    for (auto &Stream : ConsumerThreads) {
      Stream->setStopTime(StopTime);
    }
  });
}

void Topic::getPartitionsForTopic(Kafka::BrokerSettings const &Settings,
                                  std::string const &Topic) {
  try {
    auto FoundPartitions = getPartitionsForTopicInternal(
        Settings.Address, Topic, CurrentMetadataTimeOut, Settings);
    Executor.sendWork([=]() {
      CurrentMetadataTimeOut = Settings.MinMetadataTimeout;
      getOffsetsForPartitions(Settings, Topic, FoundPartitions);
    });
  } catch (MetadataException &E) {
    if (shouldGiveUp()) {
      setErrorState(fmt::format(
          R"(Meta data call for retrieving partition IDs for topic "{}" from the broker failed. The failure message was: "{}". Abandoning attempt.)",
          Topic, E.what()));
      return;
    }
    CurrentMetadataTimeOut *= 2;
    if (CurrentMetadataTimeOut > Settings.MaxMetadataTimeout) {
      CurrentMetadataTimeOut = Settings.MaxMetadataTimeout;
    }
    Logger::Info(
        R"(Meta data call for retrieving partition IDs for topic "{}" from the broker failed. The failure message was: "{}". Re-trying with a timeout of {} ms.)",
        Topic, E.what(),
        std::chrono::duration_cast<std::chrono::milliseconds>(
            CurrentMetadataTimeOut)
            .count());
    Executor.sendLowPriorityWork(
        [=]() { getPartitionsForTopic(Settings, Topic); });
  }
}

bool Topic::shouldGiveUp() {
  return getCurrentTime() > StartMetaDataTime + MetaDataGiveUp;
}

void Topic::setErrorState(const std::string &Msg) {
  HasError = true;
  std::lock_guard Lock(ErrorMsgMutex);
  ErrorMessage = Msg;
  Logger::Error(ErrorMessage);
}

std::vector<std::pair<int, int64_t>> Topic::getOffsetForTimeInternal(
    std::string const &Broker, std::string const &Topic,
    std::vector<int> const &Partitions, time_point Time, duration TimeOut,
    Kafka::BrokerSettings BrokerSettings) const {
  return _metadata_enquirer->getOffsetForTime(Broker, Topic, Partitions, Time,
                                              TimeOut, BrokerSettings);
}

std::vector<int> Topic::getPartitionsForTopicInternal(
    std::string const &Broker, std::string const &Topic, duration TimeOut,
    Kafka::BrokerSettings BrokerSettings) const {
  return _metadata_enquirer->getPartitionsForTopic(Broker, Topic, TimeOut,
                                                   BrokerSettings);
}

void Topic::getOffsetsForPartitions(Kafka::BrokerSettings const &Settings,
                                    std::string const &Topic,
                                    std::vector<int> const &Partitions) {
  try {
    auto PartitionOffsetList = getOffsetForTimeInternal(
        Settings.Address, Topic, Partitions, StartConsumeTime - StartLeeway,
        CurrentMetadataTimeOut, Settings);
    Executor.sendWork([=]() {
      CurrentMetadataTimeOut = Settings.MinMetadataTimeout;
      createStreams(Settings, Topic, PartitionOffsetList);
    });
  } catch (MetadataException &E) {
    if (shouldGiveUp()) {
      setErrorState(fmt::format(
          R"(Meta data call for retrieving offsets for topic "{}" from the broker failed. The failure message was: "{}". Abandoning attempt.)",
          Topic, E.what()));
      return;
    }
    CurrentMetadataTimeOut *= 2;
    if (CurrentMetadataTimeOut > Settings.MaxMetadataTimeout) {
      CurrentMetadataTimeOut = Settings.MaxMetadataTimeout;
    }
    Logger::Info(
        R"(Meta data call for retrieving offsets for topic "{}" from the broker failed. The failure message was: "{}". Re-trying with a timeout of {} ms.)",
        Topic, E.what(),
        std::chrono::duration_cast<std::chrono::milliseconds>(
            CurrentMetadataTimeOut)
            .count());
    Executor.sendLowPriorityWork(
        [=]() { getOffsetsForPartitions(Settings, Topic, Partitions); });
  }
}

void Topic::checkIfDoneTask() {
  Executor.sendLowPriorityWork([=]() { checkIfDone(); });
}

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

void Topic::createStreams(
    Kafka::BrokerSettings const &Settings, std::string const &Topic,
    std::vector<std::pair<int, int64_t>> const &PartitionOffsets) {
  for (const auto &[partition, offset] : PartitionOffsets) {
    auto CRegistrar =
        Registrar->getNewRegistrar("partition_" + std::to_string(partition));
    auto Consumer = _consumer_factory->createConsumerAtOffset(
        Settings, Topic, partition, offset);
    auto filters = create_filters(DataMap, StartConsumeTime, StartConsumeTime,
                                  WriterPtr, CRegistrar.get());
    auto TempPartition = std::make_unique<Partition>(
        std::move(Consumer), partition, Topic, std::move(filters),
        CRegistrar.get(), StopConsumeTime, StopLeeway,
        Settings.KafkaErrorTimeout, AreStreamersPausedFunction);
    TempPartition->start();
    ConsumerThreads.emplace_back(std::move(TempPartition));
  }
  checkIfDoneTask();
}

void Topic::checkIfDone() {
  auto const is_done =
      std::all_of(ConsumerThreads.begin(), ConsumerThreads.end(),
                  [](auto const &thread) { return thread->hasFinished(); });
  if (is_done) {
    Logger::Info("Topic {} has finished consuming.", TopicName);
    IsDone.store(true);
  }
  std::this_thread::sleep_for(50ms);
  checkIfDoneTask();
}

bool Topic::isDone() {
  if (HasError) {
    std::lock_guard Lock(ErrorMsgMutex);
    throw std::runtime_error(ErrorMessage);
  }
  return IsDone;
}

} // namespace Stream
