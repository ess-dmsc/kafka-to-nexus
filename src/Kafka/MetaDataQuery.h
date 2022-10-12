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
#include "TimeUtility.h"
#include <chrono>
#include <librdkafka/rdkafkacpp.h>
#include <set>
#include <string>

namespace Kafka {

std::vector<std::pair<int, int64_t>>
getOffsetForTime(std::string const &Broker, std::string const &Topic,
                 std::vector<int> const &Partitions, time_point Time,
                 duration TimeOut, BrokerSettings BrokerSettings);

std::vector<int> getPartitionsForTopic(std::string const &Broker,
                                       std::string const &Topic,
                                       duration TimeOut,
                                       BrokerSettings BrokerSettings);

std::set<std::string> getTopicList(std::string const &Broker, duration TimeOut,
                                   BrokerSettings BrokerSettings);

} // namespace Kafka
