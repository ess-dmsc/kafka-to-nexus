// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandListener.h"
#include "JobCreator.h"
#include "Master.h"
#include "Msg.h"
#include "Status/StatusReporter.h"
#include "helpers/FakeStreamMaster.h"
#include "helpers/KafkaWMocks.h"
#include <gtest/gtest.h>
#include <memory>

using namespace FileWriter;

std::string StartCommand{R"""({
    "cmd": "filewriter_new",
    "broker": "localhost:9092",
    "job_id": "1234",
    "file_attributes": {"file_name": "output_file1.nxs"},
    "nexus_structure": { }
  })"""};

std::string StopCommand{R"""({
    "cmd": "filewriter_stop",
    "job_id": "1234"
  })"""};

TEST(ParseCommandTests, IfCommandStringIsParseableThenDoesNotThrow) {
  ASSERT_NO_THROW(parseCommand(StartCommand));
}

TEST(ParseCommandTests, IfCommandStringIsNotParseableThenThrows) {
  ASSERT_THROW(parseCommand("{Invalid: JSON"), std::runtime_error);
}

TEST(GetNewStateTests, IfIdleThenOnStartCommandStartIsRequested) {
  FileWriterState CurrentState = States::Idle();
  auto const NewState =
      getNextState(StartCommand, std::chrono::milliseconds{0}, CurrentState);

  ASSERT_TRUE(mpark::get_if<States::StartRequested>(&NewState));
}

TEST(GetNewStateTests, IfWritingThenOnStartCommandNoStateChange) {
  FileWriterState CurrentState = States::Writing();
  auto const NewState =
      getNextState(StartCommand, std::chrono::milliseconds{0}, CurrentState);

  ASSERT_TRUE(mpark::get_if<States::Writing>(&NewState));
}

TEST(GetNewStateTests, IfWritingThenOnStopCommandStopIsRequested) {
  FileWriterState CurrentState = States::Writing();
  auto const NewState =
      getNextState(StopCommand, std::chrono::milliseconds{0}, CurrentState);

  ASSERT_TRUE(mpark::get_if<States::StopRequested>(&NewState));
}

TEST(GetNewStateTests, IfIdleThenOnStopCommandNoStateChange) {
  FileWriterState CurrentState = States::Idle();
  auto const NewState =
      getNextState(StopCommand, std::chrono::milliseconds{0}, CurrentState);

  ASSERT_TRUE(mpark::get_if<States::Idle>(&NewState));
}

class FakeJobCreator : public IJobCreator {
public:
  std::unique_ptr<IStreamMaster>
  createFileWritingJob(StartCommandInfo const & /*StartInfo*/,
                       MainOpt & /*Settings*/,
                       SharedLogger const & /*Logger*/) override {
    return std::make_unique<FakeStreamMaster>("some_id");
  };
};

class ProducerStandIn : public KafkaW::Producer {
public:
  explicit ProducerStandIn(KafkaW::BrokerSettings &Settings)
      : Producer(Settings){};
  using Producer::ProducerID;
  using Producer::ProducerPtr;
};

class ProducerTopicStandIn : public KafkaW::ProducerTopic {
public:
  ProducerTopicStandIn(std::shared_ptr<KafkaW::Producer> ProducerPtr,
                       std::string const &TopicName)
      : ProducerTopic(std::move(ProducerPtr), std::move(TopicName)){};
  int produce(const std::string & /*MsgData*/) override { return 0; }
};

class CommandListenerStandIn : public CommandListener {
public:
  explicit CommandListenerStandIn(MainOpt &Config) : CommandListener(Config){};
  void start() override{};
  std::pair<KafkaW::PollStatus, Msg> poll() override {
    return {KafkaW::PollStatus::Empty, Msg()};
  };
};

class MockMaster : public Master {
public:
  MockMaster(MainOpt Opt, std::unique_ptr<CommandListener> CmdListener,
             std::unique_ptr<IJobCreator> JobCreator,
             std::unique_ptr<Status::StatusReporter> Reporter)
      : Master(Opt, std::move(CmdListener), std::move(JobCreator),
               std::move(Reporter)) {}
  void injectMessage(KafkaW::PollStatus Status, Msg Message) {
    StoredMessage.first = Status;
    StoredMessage.second = Message;
  }

  MAKE_MOCK1(startWriting, void(StartCommandInfo const &), override);

  bool WritingStopped = false;

private:
  std::pair<KafkaW::PollStatus, Msg> pollForMessage() override {
    auto const Status = StoredMessage.first;
    StoredMessage.first = KafkaW::PollStatus::Empty;
    if (Status == KafkaW::PollStatus::Message) {
      return std::pair<KafkaW::PollStatus, Msg>(
          Status, std::move(StoredMessage.second));
    }
    return {KafkaW::PollStatus::Empty, Msg()};
  }
  std::pair<KafkaW::PollStatus, Msg> StoredMessage{KafkaW::PollStatus::Empty,
                                                   Msg()};

  bool hasWritingStopped() override { return WritingStopped; }
};

class MasterTests : public ::testing::Test {
public:
  void SetUp() override {
    MainOpt MainOpts;
    KafkaW::BrokerSettings BrokerSettings;
    std::unique_ptr<IJobCreator> Creator = std::make_unique<FakeJobCreator>();
    std::shared_ptr<KafkaW::Producer> Producer =
        std::make_shared<ProducerStandIn>(BrokerSettings);
    std::unique_ptr<CommandListener> CmdListener =
        std::make_unique<CommandListenerStandIn>(MainOpts);

    std::unique_ptr<KafkaW::ProducerTopic> ProducerTopic =
        std::make_unique<ProducerTopicStandIn>(Producer, "SomeTopic");
    auto Reporter = std::make_unique<Status::StatusReporter>(
        std::chrono::milliseconds{1000}, ProducerTopic);
    MasterPtr =
        std::make_unique<MockMaster>(MainOpts, std::move(CmdListener),
                                     std::move(Creator), std::move(Reporter));
  };

  std::unique_ptr<Master> MasterPtr;
};

TEST_F(MasterTests, IfStartCommandMessageReceivedThenEntersWritingState) {
  MockMaster *Master = dynamic_cast<MockMaster *>(MasterPtr.get());
  Master->injectMessage(KafkaW::PollStatus::Message,
                        Msg(StartCommand.c_str(), StartCommand.size()));

  REQUIRE_CALL(*Master, startWriting(trompeloeil::_));
  Master->run();
  ASSERT_TRUE(Master->isWriting());
}

TEST_F(MasterTests, IfStoppedAfterStartingThenEntersNotWritingState) {
  MockMaster *Master = dynamic_cast<MockMaster *>(MasterPtr.get());
  Master->injectMessage(KafkaW::PollStatus::Message,
                        Msg(StartCommand.c_str(), StartCommand.size()));
  REQUIRE_CALL(*Master, startWriting(trompeloeil::_));
  Master->run();

  // Stop the file-writing
  Master->WritingStopped = true;

  Master->run();
  ASSERT_FALSE(Master->isWriting());
}
