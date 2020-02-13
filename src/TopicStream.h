//
// Created by Jonas Nilsson on 2020-02-11.
//

#pragma once

#include <chrono>
#include "KafkaW/BrokerSettings.h"
#include "PartitionStream.h"
#include "ThreadedExecutor.h"
#include "logger.h"
#include "Metrics/Registrar.h"

class TopicStream {
public:
  TopicStream(KafkaW::BrokerSettings Settings, std::string Topic, time_point StartTime, Metrics::Registrar &RegisterMetric);
  void setStopTime(std::chrono::system_clock::time_point StopTime);
  ~TopicStream() = default;
protected:
  time_point BeginConsumeTime;
  std::chrono::system_clock::duration CurrentMetadataTimeOut;
  Metrics::Registrar Registrar;
  void getPartitionsForTopic(KafkaW::BrokerSettings Settings, std::string Topic);
  void getOffsetsForPartitions(KafkaW::BrokerSettings Settings, std::string Topic, std::vector<int> Partitions);
  void createStreams(KafkaW::BrokerSettings Settings, std::string Topic, std::vector<std::pair<int,int64_t>> PartitionOffsets);
  std::vector<std::unique_ptr<PartitionStream>> ConsumerThreads;
  ThreadedExecutor Executor; // Must be last
};
