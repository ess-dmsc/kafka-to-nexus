#include <memory>
#include <gtest/gtest.h>
#include "Master.h"
#include "CommandHandler.h"

// Using a mocking framework would be useful here
class MasterStandIn : public FileWriter::Master {
public:
  MasterStandIn(MainOpt &Options) : Master(Options) {};
  ~MasterStandIn() = default;
  void stop() override {
    ++CallsToStop;
    FileWriter::Master::stop();
  };
  std::vector<std::unique_ptr<FileWriter::StreamMaster<FileWriter::Streamer>>>& getStreamMasters() override {
    CallsToGetStreamMasters++;
    return FileWriter::Master::getStreamMasters();
  };
  int CallsToStop{0};
  int CallsToGetStreamMasters{0};
};

class CommandHandlerTest : public ::testing::Test {
public:
  void SetUp() override {
    TestConfigs = MainOpt();
    TestConfigs.service_id = "SomeTestId";
    MasterPtr = std::unique_ptr<FileWriter::Master>(dynamic_cast<FileWriter::Master*>(new MasterStandIn(TestConfigs)));
  }
  MainOpt TestConfigs;
  std::unique_ptr<FileWriter::Master> MasterPtr;
};

TEST_F(CommandHandlerTest, ExitSuccess) {
  std::string CommandString(R"({"cmd": "FileWriter_exit","service_id": "SomeTestId"})");
  FileWriter::CommandHandler Handler(TestConfigs, MasterPtr.get());
  Handler.tryToHandle(CommandString);
  EXPECT_EQ(dynamic_cast<MasterStandIn*>(MasterPtr.get())->CallsToStop, 1) << "Exit command was ignored.";
  FAIL() << "There should be an indciation (i.e. a response) that the file writer will exit.";
}
  
TEST_F(CommandHandlerTest, ExitFailure) {
  std::string CommandString(R"({"cmd": "FileWriter_exit","service_id": "WrongId"})");
  FileWriter::CommandHandler Handler(TestConfigs, MasterPtr.get());
  Handler.tryToHandle(CommandString);
  EXPECT_EQ(dynamic_cast<MasterStandIn*>(MasterPtr.get())->CallsToStop, 0) << "The exit command should be ignored if the service-id is wrong.";
  FAIL() << "There should be no response if the exit command is ignored and this should be testable.";
}

TEST_F(CommandHandlerTest, StopSuccess) {
  std::string CommandString(R"({"cmd": "FileWriter_stop","service_id": "SomeTestId"})");
  FileWriter::CommandHandler Handler(TestConfigs, MasterPtr.get());
  Handler.tryToHandle(CommandString);
  EXPECT_EQ(dynamic_cast<MasterStandIn*>(MasterPtr.get())->CallsToGetStreamMasters, 1) << "Stop command was ignored.";
  FAIL() << "There should be an indciation (i.e. a response) that the file writer will stop.";
}

TEST_F(CommandHandlerTest, StopFailure) {
  std::string CommandString(R"({"cmd": "FileWriter_stop","service_id": "WrongId"})");
  FileWriter::CommandHandler Handler(TestConfigs, MasterPtr.get());
  Handler.tryToHandle(CommandString);
  EXPECT_EQ(dynamic_cast<MasterStandIn*>(MasterPtr.get())->CallsToGetStreamMasters, 0) << "The stop command should be ignored if the service-id is wrong.";
  FAIL() << "There should be no response if the stop command is ignored and this should be testable.";
}

TEST_F(CommandHandlerTest, UnknownCommand) {
  std::string CommandString(R"({"cmd": "unknown_command","service_id": "SomeTestId"})");
  FileWriter::CommandHandler Handler(TestConfigs, MasterPtr.get());
  Handler.tryToHandle(CommandString);
  FAIL() << "There should be a response indicating that a command is unknown if the service-id is otherwise correct.";
}

TEST_F(CommandHandlerTest, UnknownCommandWrongId) {
  std::string CommandString(R"({"cmd": "unknown_command","service_id": "UnknownId"})");
  FileWriter::CommandHandler Handler(TestConfigs, MasterPtr.get());
  Handler.tryToHandle(CommandString);
  FAIL() << "There should be no response if the service id is wrong and this should be testable.";
}

TEST_F(CommandHandlerTest, MissingCommandFailure) {
  std::string CommandString(R"({"service_id": "SomeTestId"})");
  FileWriter::CommandHandler Handler(TestConfigs, MasterPtr.get());
  Handler.tryToHandle(CommandString);
  FAIL() << "If the command is missing, there should be a response indicating that this is the case.";
}

TEST_F(CommandHandlerTest, MissingIdFailure) {
  std::string CommandString(R"({"cmd": "FileWriter_exit"})");
  FileWriter::CommandHandler Handler(TestConfigs, MasterPtr.get());
  Handler.tryToHandle(CommandString);
  FAIL() << "There should be no response if the service-id is missing and this should be testable.";
}
