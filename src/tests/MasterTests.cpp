// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <gtest/gtest.h>
#include <memory>

#include "CommandListener.h"
#include "JobCreator.h"
#include "Master.h"
#include "Msg.h"
#include "Status/StatusReporter.h"
#include "helpers/FakeStreamMaster.h"
#include "helpers/KafkaWMocks.h"
#include "helpers/RunStartStopHelpers.h"

using namespace FileWriter;
using namespace RunStartStopHelpers;

auto const StartCommand = RunStartStopHelpers::buildRunStartMessage(
    "TEST", "42", "{}", "qw3rty", "filewriter1", "somehost:1234",
    "a-dummy-name-01.h5", 123456789000, 123456790000);

auto const StopCommand = RunStartStopHelpers::buildRunStopMessage(
    123456790000, "42", "qw3rty", "filewriter1");

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

class FakeJobCreatorThatThrows : public IJobCreator {
public:
  std::unique_ptr<IStreamMaster>
  createFileWritingJob(StartCommandInfo const & /*StartInfo*/,
                       MainOpt & /*Settings*/,
                       SharedLogger const & /*Logger*/) override {
    throw std::runtime_error("Something when wrong");
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

/// A version of Master that allows messages to be injected for testing
/// purposes.
class TestableMaster : public Master {
public:
  TestableMaster(MainOpt Opt, std::unique_ptr<CommandListener> CmdListener,
                 std::unique_ptr<IJobCreator> JobCreator,
                 std::unique_ptr<Status::StatusReporter> Reporter)
      : Master(Opt, std::move(CmdListener), std::move(JobCreator),
               std::move(Reporter)) {}
  void injectMessage(KafkaW::PollStatus const &Status, Msg const &Message) {
    StoredMessage.first = Status;
    StoredMessage.second = Message;
  }

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
    Creator = std::make_unique<FakeJobCreator>();
    ThrowingCreator = std::make_unique<FakeJobCreatorThatThrows>();
    CmdListener = std::make_unique<CommandListenerStandIn>(MainOpts);

    std::shared_ptr<KafkaW::Producer> Producer =
        std::make_shared<ProducerStandIn>(BrokerSettings);
    std::unique_ptr<KafkaW::ProducerTopic> ProducerTopic =
        std::make_unique<ProducerTopicStandIn>(Producer, "SomeTopic");
    Reporter = std::make_unique<Status::StatusReporter>(
        std::chrono::milliseconds{1000}, ProducerTopic);
  };

  MainOpt MainOpts;
  KafkaW::BrokerSettings BrokerSettings;
  std::unique_ptr<IJobCreator> Creator;
  std::unique_ptr<IJobCreator> ThrowingCreator;
  std::unique_ptr<CommandListener> CmdListener;
  std::unique_ptr<Status::StatusReporter> Reporter;
};

TEST_F(MasterTests, IfStartCommandMessageReceivedThenEntersWritingState) {
  auto Master =
      std::make_unique<TestableMaster>(MainOpts, std::move(CmdListener),
                                       std::move(Creator), std::move(Reporter));
  Master->injectMessage(KafkaW::PollStatus::Message,
                        Msg(StartCommand.data(), StartCommand.size()));

  Master->run();
  ASSERT_TRUE(Master->isWriting());
}

TEST_F(MasterTests, IfStoppedAfterStartingThenEntersNotWritingState) {
  auto Master =
      std::make_unique<TestableMaster>(MainOpts, std::move(CmdListener),
                                       std::move(Creator), std::move(Reporter));
  Master->injectMessage(KafkaW::PollStatus::Message,
                        Msg(StartCommand.data(), StartCommand.size()));
  Master->run();

  // Stop the file-writing
  Master->WritingStopped = true;

  Master->run();
  ASSERT_FALSE(Master->isWriting());
}

TEST_F(MasterTests, IfStartingThrowsThenEntersNotWritingState) {
  auto Master = std::make_unique<TestableMaster>(
      MainOpts, std::move(CmdListener), std::move(ThrowingCreator),
      std::move(Reporter));
  Master->injectMessage(KafkaW::PollStatus::Message,
                        Msg(StartCommand.data(), StartCommand.size()));

  Master->run();

  ASSERT_FALSE(Master->isWriting());
}
