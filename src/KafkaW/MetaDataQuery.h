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

// \brief Query the broker for topic + partition offset of a given timestamp.
//
// \param[in] Broker    Address of one or several brokers.
// \param[in] Topic     Name of topic.
// \param[in] Partition Partition id.
// \param[in] Time      Timestamp for which the offset should be found.
// \param[in] TimeOut   The amount of time to wait for the meta-data call to
// complete. \returns             The closest offset with a timestamp before
// Time (if available). \throws §MetadataException if a failure occurs.
std::vector<std::pair<int, int64_t>>
getOffsetForTime(std::string const &Broker, std::string const &Topic,
                 std::vector<int> const &Partitions, time_point Time,
                 duration TimeOut);

// \brief Query the broker for partitions available on a specified topic.
//
// \param[in] Broker    Address of one or several brokers.
// \param[in] Topic     Name of topic.
// \param[in] TimeOut   The amount of time to wait for the meta-data call to
// complete. \returns             A set of partition id:s. \throws
// MetadataException if a failure occurs.
std::vector<int> getPartitionsForTopic(std::string const &Broker,
                                       std::string const &Topic,
                                       duration TimeOut);

// \brief Query the broker for available topics.
//
// \param[in] Broker    Address of one or several brokers.
// \param[in] TimeOut   The amount of time to wait for the meta-data call to
// complete. \returns             A set of topic names. \throws
// MetadataException if a failure occurs.
std::set<std::string> getTopicList(std::string const &Broker, duration TimeOut);

} // namespace KafkaW
