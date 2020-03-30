// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <chrono>
#include <librdkafka/rdkafkacpp.h>
#include <set>
#include <string>

namespace KafkaW {

using time_point = std::chrono::system_clock::time_point;
using duration = std::chrono::system_clock::duration;

std::vector<std::pair<int, int64_t>>
getOffsetForTime(std::string const &Broker, std::string const &Topic,
                 std::vector<int> const &Partitions, time_point Time,
                 duration TimeOut);

std::vector<int> getPartitionsForTopic(std::string const &Broker,
                                       std::string const &Topic,
                                       duration TimeOut);

std::set<std::string> getTopicList(std::string const &Broker, duration TimeOut);

} // namespace KafkaW
