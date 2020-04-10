// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <trompeloeil.hpp>

namespace Kafka {

class MockConsumer
    : public trompeloeil::mock_interface<Kafka::ConsumerInterface> {
public:
  explicit MockConsumer(const Kafka::BrokerSettings &Settings){
      UNUSED_ARG(Settings)};
  using PollReturnType = std::pair<Kafka::PollStatus, FileWriter::Msg>;
  IMPLEMENT_MOCK1(addTopic);
  IMPLEMENT_MOCK2(addTopicAtTimestamp);
  IMPLEMENT_MOCK1(topicPresent);
  IMPLEMENT_MOCK1(queryTopicPartitions);
  IMPLEMENT_MOCK0(poll);
  IMPLEMENT_MOCK2(offsetsForTimesAllPartitions);
  IMPLEMENT_MOCK2(getHighWatermarkOffset);
  IMPLEMENT_MOCK1(getCurrentOffsets);
  IMPLEMENT_MOCK3(addPartitionAtOffset);
};

} // namespace Kafka
