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
#include "Status/StatusInfo.h"
#include "Status/StatusReporter.h"
#include "helpers/FakeStreamController.h"
#include "helpers/RdKafkaMocks.h"
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
  std::unique_ptr<IStreamController>
  createFileWritingJob(StartCommandInfo const &StartInfo,
                       MainOpt & /*Settings*/, SharedLogger const & /*Logger*/,
                       Metrics::Registrar) override {
    return std::make_unique<FakeStreamController>(StartInfo.JobID);
  };
};

class FakeJobCreatorThatThrows : public IJobCreator {
public:
  std::unique_ptr<IStreamController>
  createFileWritingJob(StartCommandInfo const & /*StartInfo*/,
                       MainOpt & /*Settings*/, SharedLogger const & /*Logger*/,
                       Metrics::Registrar) override {
    throw std::runtime_error("Something went wrong");
  };
};

class ProducerStandIn : public Kafka::Producer {
public:
  explicit ProducerStandIn(Kafka::BrokerSettings &Settings)
      : Producer(Settings){};
  using Producer::ProducerID;
  using Producer::ProducerPtr;
};

class ProducerTopicStandIn : public Kafka::ProducerTopic {
public:
  ProducerTopicStandIn(std::shared_ptr<Kafka::Producer> ProducerPtr,
                       std::string TopicName)
      : ProducerTopic(std::move(ProducerPtr), std::move(TopicName)){};
  int produce(flatbuffers::DetachedBuffer const & /*Msg*/) override {
    return 0;
  }
};

class FakeCommandListener : public CommandListener {
public:
  explicit FakeCommandListener(MainOpt &Config) : CommandListener(Config){};
  void start() override{};
  std::pair<Kafka::PollStatus, Msg> poll() override {
    if (StoredMessages.empty()) {
      return {Kafka::PollStatus::Empty, Msg()};
    }
    auto Message = std::move(StoredMessages.front());
    StoredMessages.pop_front();
    return Message;
  };

  void injectMessage(Kafka::PollStatus const &Status, Msg const &Message) {
    std::pair<Kafka::PollStatus, Msg> NewMsg;
    NewMsg.first = Status;
    NewMsg.second = Message;
    StoredMessages.emplace_back(std::move(NewMsg));
  }

  std::deque<std::pair<Kafka::PollStatus, Msg>> StoredMessages;
};

class MasterTests : public ::testing::Test {
public:
  void SetUp() override {
    Creator = std::make_unique<FakeJobCreator>();
    ThrowingCreator = std::make_unique<FakeJobCreatorThatThrows>();
    CmdListener = std::make_unique<FakeCommandListener>(MainOpts);

    std::shared_ptr<Kafka::Producer> Producer =
        std::make_shared<ProducerStandIn>(BrokerSettings);
    std::unique_ptr<Kafka::ProducerTopic> ProducerTopic =
        std::make_unique<ProducerTopicStandIn>(Producer, "SomeTopic");
    Status::ApplicationStatusInfo const TestStatusInformation =
        Status::ApplicationStatusInfo{std::chrono::milliseconds(1000),
                                      "test_application",
                                      "test_version",
                                      "test_host_name",
                                      "test_service_id",
                                      0};
    Reporter = std::make_unique<Status::StatusReporter>(ProducerTopic,
                                                        TestStatusInformation);
  };

  MainOpt MainOpts;
  Kafka::BrokerSettings BrokerSettings;
  std::unique_ptr<IJobCreator> Creator;
  std::unique_ptr<IJobCreator> ThrowingCreator;
  std::unique_ptr<CommandListener> CmdListener;
  std::unique_ptr<Status::StatusReporter> Reporter;
};

void queueCommandMessage(CommandListener *Listener, Kafka::PollStatus Status,
                         Msg Message) {
  dynamic_cast<FakeCommandListener *>(Listener)->injectMessage(Status, Message);
}

TEST_F(MasterTests, IfStartCommandMessageReceivedThenEntersWritingState) {
  queueCommandMessage(CmdListener.get(), Kafka::PollStatus::Message,
                      Msg(StartCommand.data(), StartCommand.size()));

  auto Master = std::make_unique<FileWriter::Master>(
      MainOpts, std::move(CmdListener), std::move(Creator), std::move(Reporter),
      Metrics::Registrar("some_reg", {}));

  Master->run();
  ASSERT_TRUE(Master->isWriting());
}

TEST_F(MasterTests, IfStoppedAfterStartingThenEntersNotWritingState) {
  queueCommandMessage(CmdListener.get(), Kafka::PollStatus::Message,
                      Msg(StartCommand.data(), StartCommand.size()));
  queueCommandMessage(CmdListener.get(), Kafka::PollStatus::Message,
                      Msg(StopCommand.data(), StopCommand.size()));

  auto Master = std::make_unique<FileWriter::Master>(
      MainOpts, std::move(CmdListener), std::move(Creator), std::move(Reporter),
      Metrics::Registrar("some_reg", {}));
  // Process start message
  Master->run();

  // Process stop message
  Master->run();
  ASSERT_FALSE(Master->isWriting());
}

TEST_F(MasterTests, IfStartingThrowsThenEntersNotWritingState) {
  queueCommandMessage(CmdListener.get(), Kafka::PollStatus::Message,
                      Msg(StartCommand.data(), StartCommand.size()));

  auto Master = std::make_unique<FileWriter::Master>(
      MainOpts, std::move(CmdListener), std::move(ThrowingCreator),
      std::move(Reporter), Metrics::Registrar("some_reg", {}));

  Master->run();

  ASSERT_FALSE(Master->isWriting());
}

TEST_F(MasterTests, IfStoppedMessageContainsWrongJobIdThenIgnored) {
  auto WrongIdCommand = RunStartStopHelpers::buildRunStopMessage(
      123456790000, "42", "wrong_id", "filewriter1");

  queueCommandMessage(CmdListener.get(), Kafka::PollStatus::Message,
                      Msg(StartCommand.data(), StartCommand.size()));
  queueCommandMessage(CmdListener.get(), Kafka::PollStatus::Message,
                      Msg(WrongIdCommand.data(), WrongIdCommand.size()));

  auto Master = std::make_unique<FileWriter::Master>(
      MainOpts, std::move(CmdListener), std::move(Creator), std::move(Reporter),
      Metrics::Registrar("some_reg", {}));
  // Process start message
  Master->run();

  // Process stop message
  Master->run();
  ASSERT_TRUE(Master->isWriting());
}
