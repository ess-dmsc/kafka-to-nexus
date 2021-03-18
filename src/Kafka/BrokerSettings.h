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
#include <map>
#include <string>

namespace Kafka {

using duration = std::chrono::system_clock::duration;
using std::chrono_literals::operator""s;
using std::chrono_literals::operator""ms;
/// Collect options used to connect to the broker.
struct BrokerSettings {
  BrokerSettings() = default;
  std::string Address;
  int PollTimeoutMS = 500;
  duration MinMetadataTimeout{
      400ms}; // When doing Kafka metadata calls, start with this timeout
  duration MaxMetadataTimeout{
      10s}; // When doing Kafka metadata calls, use this as the max timeout
  duration KafkaErrorTimeout{
      30s}; // If there is an error with the Kafka broker when consuming data
            // (for writing files), wait this long before stopping
  std::map<std::string, std::string> KafkaConfiguration = {
      {"socket.timeout.ms", "10000"},
      {"message.max.bytes", "24000000"},
      {"fetch.message.max.bytes", "24000000"},
      {"fetch.max.bytes",
       "52428800"}, // this is the default value, here as documentation
      {"receive.message.max.bytes",
       "53428800"}, // must be at least fetch.max.bytes + 512
      {"queue.buffering.max.messages", "100000"},
      {"queue.buffering.max.ms", "50"},
      {"queue.buffering.max.kbytes", "819200"}, // 819.2 Mib
      {"batch.num.messages", "100000"},
      {"coordinator.query.interval.ms", "5000"},
      {"heartbeat.interval.ms", "500"},     // 0.5 Secs
      {"statistics.interval.ms", "600000"}, // 1 Min
      {"api.version.request", "true"},
      {"enable.auto.commit", "false"},
      {"enable.partition.eof", "true"}};
};
} // namespace Kafka
