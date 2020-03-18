// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "KafkaW/BrokerSettings.h"
#include "Metrics/Registrar.h"
#include "Partition.h"
#include "Stream/MessageWriter.h"
#include "ThreadedExecutor.h"
#include "logger.h"
#include <chrono>
#include <set>

namespace Stream {

class Topic {
public:
  Topic(KafkaW::BrokerSettings const &Settings, std::string const &Topic,
        SrcToDst Map, MessageWriter *Writer, Metrics::Registrar &RegisterMetric,
        time_point StartTime, duration StartTimeLeeway, time_point StopTime,
        duration StopTimeLeeway);

  /// \brief Must be called after the constructor.
  /// \note This function exist in order to make unit testing possible.
  void start();

  void setStopTime(std::chrono::system_clock::time_point StopTime);

  virtual ~Topic() = default;

protected:
  KafkaW::BrokerSettings KafkaSettings;
  std::string TopicName;
  SrcToDst DataMap;
  MessageWriter *WriterPtr;
  time_point StartConsumeTime;
  duration StartLeeway;
  time_point StopConsumeTime;
  duration StopLeeway;
  duration KafkaErrorTimeout;
  duration CurrentMetadataTimeOut;
  Metrics::Registrar Registrar;

  // This intermediate function is required for unit testing.
  virtual void initMetadataCalls(KafkaW::BrokerSettings Settings,
                                 std::string Topic);

  virtual void getPartitionsForTopic(KafkaW::BrokerSettings Settings,
                                     std::string Topic);

  virtual void getOffsetsForPartitions(KafkaW::BrokerSettings Settings,
                                       std::string Topic,
                                       std::vector<int> Partitions);

  virtual void
  createStreams(KafkaW::BrokerSettings Settings, std::string Topic,
                std::vector<std::pair<int, int64_t>> PartitionOffsets);

  virtual std::vector<std::pair<int, int64_t>>
  getOffsetForTimeInternal(std::string Broker, std::string Topic,
                           std::vector<int> Partitions, time_point Time,
                           duration TimeOut) const;

  virtual std::vector<int>
  getPartitionsForTopicInternal(std::string Broker, std::string Topic,
                                duration TimeOut) const;

  std::vector<std::unique_ptr<Partition>> ConsumerThreads;
  ThreadedExecutor Executor; // Must be last
};
} // namespace Stream