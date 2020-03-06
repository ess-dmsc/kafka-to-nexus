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

namespace Stream {

Topic::Topic(KafkaW::BrokerSettings Settings, std::string Topic, SrcToDst Map,
             MessageWriter *Writer, Metrics::Registrar &RegisterMetric,
             time_point StartTime, duration StartTimeLeeway, time_point StopTime, duration StopTimeLeeway)
    : DataMap(Map), WriterPtr(Writer), StartConsumeTime(StartTime), StartLeeway(StartTimeLeeway),
      StopConsumeTime(StopTime), StopLeeway(StopTimeLeeway), KafkaErrorTimeout(Settings.KafkaErrorTimeout),
      CurrentMetadataTimeOut(Settings.MinMetadataTimeout),
      Registrar(RegisterMetric.getNewRegistrar(Topic)) {
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
  std::vector<int> FoundPartitions;
  try {
    auto FoundPartitions = KafkaW::getPartitionsForTopic(
        Settings.Address, Topic, Settings.MinMetadataTimeout);
  } catch (std::exception &E) {
    CurrentMetadataTimeOut *= 2;
    if (CurrentMetadataTimeOut > Settings.MaxMetadataTimeout) {
      CurrentMetadataTimeOut = Settings.MaxMetadataTimeout;
    }
    Executor.SendLowPrioWork([=]() { getPartitionsForTopic(Settings, Topic); });
  }
  Executor.SendWork([=]() {
    CurrentMetadataTimeOut = Settings.MinMetadataTimeout;
    getOffsetsForPartitions(Settings, Topic, FoundPartitions);
  });
}

void Topic::getOffsetsForPartitions(KafkaW::BrokerSettings Settings,
                                    std::string Topic,
                                    std::vector<int> Partitions) {
  std::vector<std::pair<int, int64_t>> PartitionOffsetList;
  try {
    PartitionOffsetList =
        KafkaW::getOffsetForTime(Settings.Address, Topic, Partitions,
                                 StartConsumeTime - StartLeeway, CurrentMetadataTimeOut);
  } catch (std::exception &E) {
    CurrentMetadataTimeOut *= 2;
    if (CurrentMetadataTimeOut > Settings.MaxMetadataTimeout) {
      CurrentMetadataTimeOut = Settings.MaxMetadataTimeout;
    }
    Executor.SendLowPrioWork(
        [=]() { getOffsetsForPartitions(Settings, Topic, Partitions); });
  }
  Executor.SendWork(
      [=]() { createStreams(Settings, Topic, PartitionOffsetList); });
}

void Topic::createStreams(
    KafkaW::BrokerSettings Settings, std::string Topic,
    std::vector<std::pair<int, int64_t>> PartitionOffsets) {
  for (const auto &CParOffset : PartitionOffsets) {
    auto CRegistrar = Registrar.getNewRegistrar(
        "partition_" + std::to_string(CParOffset.first));
    auto Consumer = KafkaW::createConsumer(Settings);
    Consumer->addPartitionAtOffset(Topic, CParOffset.first, CParOffset.second);
    ConsumerThreads.emplace_back(std::make_unique<Partition>(
        std::move(Consumer), DataMap, WriterPtr, CRegistrar, StartConsumeTime,
        StopConsumeTime, StopLeeway, KafkaErrorTimeout));
  }
}

} // namespace Stream
