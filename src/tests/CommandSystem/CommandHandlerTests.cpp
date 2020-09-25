// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandSystem/Handler.h"
#include "CommandSystem/JobListener.h"
#include "CommandSystem/ResponseProducerBase.h"
#include "CommandSystem/CommandListener.h"
#include <gtest/gtest.h>
#include <trompeloeil.hpp>
#include <memory>

using trompeloeil::_;

class ResponseProducerStandIn : public Command::ResponseProducerBase {
public:
  ResponseProducerStandIn() : Command::ResponseProducerBase() {}
  MAKE_MOCK5(publishResponse, void(Command::ActionResponse, Command::ActionResult, std::string, std::string, std::string), override);
};

using PollResult = std::pair<Kafka::PollStatus, FileWriter::Msg>;

class ListenerStandIn : public Command::JobListener {
public:
  ListenerStandIn() : Command::JobListener(uri::URI{"localhost/test"}, {}) {}
  MAKE_MOCK0(pollForJob, PollResult(), override);
  MAKE_MOCK0(pollForCommand, PollResult(), override);
  MAKE_MOCK0(disconnectFromPool, void(), override);
};


class CommandHandlerTests : public ::testing::Test {
protected:
  void SetUp() override {
    MockResponseProducer = new ResponseProducerStandIn;
    MockJobListener = new ListenerStandIn;
    MockCmdListener = new ListenerStandIn;
    UnderTest = std::make_unique<Command::Handler>(ServiceIdentifier, std::unique_ptr<Command::JobListener>(MockJobListener), std::unique_ptr<Command::CommandListener>(MockCmdListener), std::unique_ptr<Command::ResponseProducerBase>(MockResponseProducer));
  }
  void TearDown() override {
    UnderTest.reset();
  }
  std::string const ServiceIdentifier{"test_identifier"};
  std::unique_ptr<Command::Handler> UnderTest;
  ResponseProducerStandIn *MockResponseProducer;
  ListenerStandIn *MockJobListener;
  ListenerStandIn *MockCmdListener;
};

TEST_F(CommandHandlerTests, SomeTest) {

}
