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

namespace Stream {

class Topic {
public:
  Topic(KafkaW::BrokerSettings Settings, std::string Topic, SrcToDst Map,
        MessageWriter *Writer, Metrics::Registrar &RegisterMetric,
        time_point StartTime,
        time_point StopTime = std::chrono::system_clock::time_point::max());

  void setStopTime(std::chrono::system_clock::time_point StopTime);

  ~Topic() = default;

protected:
  SrcToDst DataMap;
  MessageWriter *WriterPtr;
  time_point StartConsumeTime;
  time_point StopConsumeTime;
  std::chrono::system_clock::duration CurrentMetadataTimeOut;
  Metrics::Registrar Registrar;

  void getPartitionsForTopic(KafkaW::BrokerSettings Settings,
                             std::string Topic);

  void getOffsetsForPartitions(KafkaW::BrokerSettings Settings,
                               std::string Topic, std::vector<int> Partitions);

  void createStreams(KafkaW::BrokerSettings Settings, std::string Topic,
                     std::vector<std::pair<int, int64_t>> PartitionOffsets);

  std::vector<std::unique_ptr<Partition>> ConsumerThreads;
  ThreadedExecutor Executor; // Must be last
};
} // namespace Stream