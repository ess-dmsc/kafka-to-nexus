// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Topic.h"
#include "Kafka/ConsumerFactory.h"
#include "Kafka/MetaDataQuery.h"
#include "logger.h"
#include <Kafka/MetadataException.h>

namespace Stream {

Topic::Topic(Kafka::BrokerSettings const &Settings, std::string const &Topic,
             SrcToDst Map, MessageWriter *Writer,
             Metrics::Registrar &RegisterMetric, time_point StartTime,
             duration StartTimeLeeway, time_point StopTime,
             duration StopTimeLeeway,
             std::unique_ptr<Kafka::ConsumerFactoryInterface> CreateConsumers)
    : KafkaSettings(Settings), TopicName(Topic), DataMap(std::move(Map)),
      WriterPtr(Writer), StartConsumeTime(StartTime),
      StartLeeway(StartTimeLeeway), StopConsumeTime(StopTime),
      StopLeeway(StopTimeLeeway),
      CurrentMetadataTimeOut(Settings.MinMetadataTimeout),
      Registrar(RegisterMetric.getNewRegistrar(Topic)),
      ConsumerCreator(std::move(CreateConsumers)) {}

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
        Settings.Address, Topic, CurrentMetadataTimeOut);
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
    LOG_WARN(R"(Meta data call for retrieving partition IDs for topic "{}" from the broker failed. The failure message was: "{}". Re-trying with a timeout of {} ms.)",
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
  LOG_ERROR(ErrorMessage);
  return;
}

std::vector<std::pair<int, int64_t>>
Topic::getOffsetForTimeInternal(std::string const &Broker,
                                std::string const &Topic,
                                std::vector<int> const &Partitions,
                                time_point Time, duration TimeOut) const {
  return Kafka::getOffsetForTime(Broker, Topic, Partitions, Time, TimeOut);
}

std::vector<int> Topic::getPartitionsForTopicInternal(std::string const &Broker,
                                                      std::string const &Topic,
                                                      duration TimeOut) const {
  return Kafka::getPartitionsForTopic(Broker, Topic, TimeOut);
}

void Topic::getOffsetsForPartitions(Kafka::BrokerSettings const &Settings,
                                    std::string const &Topic,
                                    std::vector<int> const &Partitions) {
  try {
    auto PartitionOffsetList = getOffsetForTimeInternal(
        Settings.Address, Topic, Partitions, StartConsumeTime - StartLeeway,
        CurrentMetadataTimeOut);
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
    LOG_WARN(R"(Meta data call for retrieving offsets for topic "{}" from the broker failed. The failure message was: "{}". Re-trying with a timeout of {} ms.)",
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

void Topic::createStreams(
    Kafka::BrokerSettings const &Settings, std::string const &Topic,
    std::vector<std::pair<int, int64_t>> const &PartitionOffsets) {
  for (const auto &CParOffset : PartitionOffsets) {
    auto CRegistrar = Registrar.getNewRegistrar(
        "partition_" + std::to_string(CParOffset.first));
    auto Consumer = ConsumerCreator->createConsumer(Settings);
    Consumer->addPartitionAtOffset(Topic, CParOffset.first, CParOffset.second);
    auto TempPartition = std::make_unique<Partition>(
        std::move(Consumer), CParOffset.first, Topic, DataMap, WriterPtr,
        CRegistrar, StartConsumeTime, StopConsumeTime, StopLeeway,
        Settings.KafkaErrorTimeout);
    TempPartition->start();
    ConsumerThreads.emplace_back(std::move(TempPartition));
  }
  checkIfDoneTask();
}

void Topic::checkIfDone() {
  ConsumerThreads.erase(
      std::remove_if(ConsumerThreads.begin(), ConsumerThreads.end(),
                     [](auto const &Elem) { return Elem->hasFinished(); }),
      ConsumerThreads.end());
  if (ConsumerThreads.empty()) {
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
