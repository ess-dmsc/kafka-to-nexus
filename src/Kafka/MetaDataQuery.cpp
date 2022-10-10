// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Kafka/MetaDataQuery.h"
#include "Kafka/MetaDataQueryImpl.h"
#include "Kafka/MetadataException.h"

namespace Kafka {

std::vector<std::pair<int, int64_t>>
getOffsetForTime(std::string const &Broker, std::string const &Topic,
                 std::vector<int> const &Partitions, time_point Time,
                 duration TimeOut, BrokerSettings BrokerSettings) {
  return getOffsetForTimeImpl<RdKafka::Consumer>(Broker, Topic, Partitions,
                                                 Time, TimeOut, BrokerSettings);
}

std::vector<int> getPartitionsForTopic(std::string const &Broker,
                                       std::string const &Topic,
                                       duration TimeOut,
                                       BrokerSettings BrokerSettings) {
  return getPartitionsForTopicImpl<RdKafka::Consumer, RdKafka::Topic>(
      Broker, Topic, TimeOut, BrokerSettings);
}

std::set<std::string> getTopicList(std::string const &Broker,
                                   duration TimeOut, BrokerSettings BrokerSettings) {
  return getTopicListImpl<RdKafka::Consumer>(Broker, TimeOut, BrokerSettings);
}

} // namespace Kafka
