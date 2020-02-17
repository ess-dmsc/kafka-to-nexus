// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Topic.h"
#include "KafkaW/MetaDataQuery.h"
#include "KafkaW/ConsumerFactory.h"

namespace Stream {

Topic::Topic(KafkaW::BrokerSettings Settings, std::string Topic,
             time_point StartTime,
             Metrics::Registrar &RegisterMetric) : BeginConsumeTime(
    StartTime), CurrentMetadataTimeOut(Settings.MinMetadataTimeout), Registrar(
    RegisterMetric.getNewRegistrar(Topic)) {
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
    auto FoundPartitions = KafkaW::getPartitionsForTopic(Settings.Address,
                                                         Topic,
                                                         Settings.MinMetadataTimeout);
  } catch (std::exception &E) {
    CurrentMetadataTimeOut *= 2;
    if (CurrentMetadataTimeOut > Settings.MaxMetadataTimeout) {
      CurrentMetadataTimeOut = Settings.MaxMetadataTimeout;
    }
    Executor.SendWork([=]() {
      getPartitionsForTopic(Settings, Topic);
    });
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
    PartitionOffsetList = KafkaW::getOffsetForTime(Settings.Address, Topic,
                                                   Partitions, BeginConsumeTime,
                                                   CurrentMetadataTimeOut);
  } catch (std::exception &E) {
    CurrentMetadataTimeOut *= 2;
    if (CurrentMetadataTimeOut > Settings.MaxMetadataTimeout) {
      CurrentMetadataTimeOut = Settings.MaxMetadataTimeout;
    }
    Executor.SendWork([=]() {
      getOffsetsForPartitions(Settings, Topic, Partitions);
    });
  }
  Executor.SendWork([=]() {
    createStreams(Settings, Topic, PartitionOffsetList);
  });
}

void
Topic::createStreams(KafkaW::BrokerSettings Settings, std::string Topic,
                     std::vector<std::pair<int, int64_t> > PartitionOffsets) {
  for (const auto &CParOffset : PartitionOffsets) {
    auto CRegistrar = Registrar.getNewRegistrar(
        "partitions_" + std::to_string(CParOffset.first));
    auto Consumer = KafkaW::createConsumer(Settings);
    Consumer->addPartitionAtOffset(Topic, CParOffset.first, CParOffset.second);
    ConsumerThreads.emplace_back(
        std::make_unique<Partition>(std::move(Consumer), SrcToDst(),
                                    CRegistrar));
  }
}

} // namespace Stream
