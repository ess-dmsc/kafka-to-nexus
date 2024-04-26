// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Kafka/BrokerSettings.h"
#include "Kafka/MetadataException.h"
#include "TimeUtility.h"
#include <chrono>
#include <fmt/format.h>
#include <librdkafka/rdkafkacpp.h>
#include <set>
#include <string>

namespace Kafka {

class MetadataEnquirer {
public:
  virtual ~MetadataEnquirer() = default;

  virtual std::vector<std::pair<int, int64_t>>
  getOffsetForTime(std::string const &Broker, std::string const &Topic,
                   std::vector<int> const &Partitions, time_point Time,
                   duration TimeOut, BrokerSettings const &BrokerSettings);

  virtual std::vector<int>
  getPartitionsForTopic(std::string const &Broker, std::string const &Topic,
                        duration TimeOut, BrokerSettings const &BrokerSettings);

  virtual std::set<std::string>
  getTopicList(std::string const &Broker, duration TimeOut,
               BrokerSettings const &BrokerSettings);

private:
  const RdKafka::TopicMetadata *
  findTopicMetadata(const std::string &Topic,
                    const RdKafka::Metadata *KafkaMetadata);

  std::unique_ptr<RdKafka::Consumer>
  getKafkaHandle(std::string const &Broker,
                 BrokerSettings const &BrokerSettings);

  std::vector<int>
  extractPartitionIDs(RdKafka::TopicMetadata const *TopicMetaData);
};
} // namespace Kafka
