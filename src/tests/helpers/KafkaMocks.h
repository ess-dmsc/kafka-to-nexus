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
  IMPLEMENT_MOCK0(poll);
  IMPLEMENT_MOCK3(addPartitionAtOffset);
  IMPLEMENT_MOCK2(addTopic);
};

} // namespace Kafka
