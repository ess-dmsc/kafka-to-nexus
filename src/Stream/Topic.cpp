// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Topic.h"
#include "KafkaW/ConsumerFactory.h"
#include "KafkaW/MetaDataQuery.h"
#include "logger.h"
#include <KafkaW/MetadataException.h>

namespace Stream {

Topic::Topic(KafkaW::BrokerSettings const &Settings, std::string const &Topic,
             SrcToDst Map, MessageWriter *Writer,
             Metrics::Registrar &RegisterMetric, time_point StartTime,
             duration StartTimeLeeway, time_point StopTime,
             duration StopTimeLeeway)
    : KafkaSettings(Settings), TopicName(Topic), DataMap(std::move(Map)),
      WriterPtr(Writer), StartConsumeTime(StartTime),
      StartLeeway(StartTimeLeeway), StopConsumeTime(StopTime),
      StopLeeway(StopTimeLeeway), KafkaErrorTimeout(Settings.KafkaErrorTimeout),
      CurrentMetadataTimeOut(Settings.MinMetadataTimeout),
      Registrar(RegisterMetric.getNewRegistrar(Topic)) {}

void Topic::start() {
  Executor.SendWork([=]() { initMetadataCalls(KafkaSettings, TopicName); });
}

void Topic::initMetadataCalls(KafkaW::BrokerSettings Settings,
                              std::string Topic) {
  Executor.SendWork([=]() {
    CurrentMetadataTimeOut = Settings.MinMetadataTimeout;
    getPartitionsForTopic(Settings, Topic);
  });
}

void Topic::setStopTime(std::chrono::system_clock::time_point StopTime) {
  for (auto &Stream : ConsumerThreads) {
    Stream->setStopTime(StopTime);
  }
}

void Topic::getPartitionsForTopic(KafkaW::BrokerSettings Settings,
                                  std::string Topic) {
  try {
    auto FoundPartitions = getPartitionsForTopicInternal(
        Settings.Address, Topic, Settings.MinMetadataTimeout);
    Executor.SendWork([=]() {
      CurrentMetadataTimeOut = Settings.MinMetadataTimeout;
      getOffsetsForPartitions(Settings, Topic, FoundPartitions);
    });
  } catch (MetadataException &E) {
    CurrentMetadataTimeOut *= 2;
    if (CurrentMetadataTimeOut > Settings.MaxMetadataTimeout) {
      CurrentMetadataTimeOut = Settings.MaxMetadataTimeout;
    }
    LOG_WARN("Meta data call for retrieving partition IDs for topic \"{}\" "
             "from the broker "
             "failed. The failure message was: \"{}\". Re-trying with a "
             "timeout of {} ms.",
             Topic, E.what(),
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 CurrentMetadataTimeOut)
                 .count());
    Executor.SendLowPrioWork([=]() { getPartitionsForTopic(Settings, Topic); });
  }
}

std::vector<std::pair<int, int64_t>>
Topic::getOffsetForTimeInternal(std::string Broker, std::string Topic,
                                std::vector<int> Partitions, time_point Time,
                                duration TimeOut) const {
  return KafkaW::getOffsetForTime(Broker, Topic, Partitions, Time, TimeOut);
}

std::vector<int> Topic::getPartitionsForTopicInternal(std::string Broker,
                                                      std::string Topic,
                                                      duration TimeOut) const {
  return KafkaW::getPartitionsForTopic(Broker, Topic, TimeOut);
}

void Topic::getOffsetsForPartitions(KafkaW::BrokerSettings Settings,
                                    std::string Topic,
                                    std::vector<int> Partitions) {
  try {
    auto PartitionOffsetList = getOffsetForTimeInternal(
        Settings.Address, Topic, Partitions, StartConsumeTime - StartLeeway,
        CurrentMetadataTimeOut);
    Executor.SendWork([=]() {
      CurrentMetadataTimeOut = Settings.MinMetadataTimeout;
      createStreams(Settings, Topic, PartitionOffsetList);
    });
  } catch (MetadataException &E) {
    CurrentMetadataTimeOut *= 2;
    if (CurrentMetadataTimeOut > Settings.MaxMetadataTimeout) {
      CurrentMetadataTimeOut = Settings.MaxMetadataTimeout;
    }
    LOG_WARN("Meta data call for retrieving offsets for topic \"{}\" from the "
             "broker "
             "failed. The failure message was: \"{}\". Re-trying with a "
             "timeout of {} ms.",
             Topic, E.what(),
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 CurrentMetadataTimeOut)
                 .count());
    Executor.SendLowPrioWork(
        [=]() { getOffsetsForPartitions(Settings, Topic, Partitions); });
  }
}

void Topic::createStreams(
    KafkaW::BrokerSettings Settings, std::string Topic,
    std::vector<std::pair<int, int64_t>> PartitionOffsets) {
  for (const auto &CParOffset : PartitionOffsets) {
    auto CRegistrar = Registrar.getNewRegistrar(
        "partition_" + std::to_string(CParOffset.first));
    auto Consumer = KafkaW::createConsumer(Settings);
    Consumer->addPartitionAtOffset(Topic, CParOffset.first, CParOffset.second);
    auto TempPartition = std::make_unique<Partition>(
        std::move(Consumer), CParOffset.first, Topic, DataMap, WriterPtr,
        CRegistrar, StartConsumeTime, StopConsumeTime, StopLeeway,
        KafkaErrorTimeout);
    TempPartition->start();
    ConsumerThreads.emplace_back(std::move(TempPartition));
  }
}
} // namespace Stream
