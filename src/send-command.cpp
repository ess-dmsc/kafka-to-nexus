#include "CLIOptions.h"
#include "KafkaW/ProducerTopic.h"
#include "URI.h"
#include "helper.h"
#include "json.h"
#include <CLI/CLI.hpp>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>

using uri::URI;
using nlohmann::json;

// POD
struct MainOpt {
  uint64_t teamid = 0;
  URI broker{"localhost:9092/commands"};
  KafkaW::BrokerSettings BrokerSettings;
  std::string cmd;
  spdlog::level::level_enum LoggingLevel;
  std::string LogFilename;
  uri::URI GraylogLoggerAddress;
};

std::string make_command(const std::string &broker, const uint64_t &teamid) {
  auto Command = json::parse(R""({
    "cmd": "FileWriter_new",
    "streams": [
      {}
    ]
  })"");
  Command["teamid"] = teamid;
  Command["broker"] = broker;
  Command["filename"] = fmt::format("tmp-{:016x}.h5", teamid);
  Command["streams"][0]["broker"] = broker;
  Command["streams"][0]["topic"] = "topic.with.multiple.sources";
  Command["streams"][0]["source"] = "source-00";
  return Command.dump();
}

std::string make_command_exit(const std::string &broker,
                              const uint64_t &teamid) {
  auto Command = json::parse(R""({
    "cmd": "FileWriter_exit"
  })"");
  Command["teamid"] = teamid;
  return Command.dump();
}

std::string make_command_stop(
    const std::string &broker, const std::string &job_id,
    const std::chrono::milliseconds &stop_time = std::chrono::milliseconds{0}) {
  auto Command = json::parse(R""({
    "cmd": "FileWriter_stop"
  })"");
  Command["job_id"] = job_id;
  if (stop_time.count() != 0) {
    Command["stop_time"] = stop_time.count();
  }
  return Command.dump();
}

std::string make_command_from_file(const std::string &filename) {
  auto Logger = getLogger();
  std::ifstream ifs(filename);
  if (!ifs.good()) {
    Logger->warn("can not open file {}", filename);
    return "";
  }
  Logger->trace("make_command_from_file {}", filename);
  auto buf1 = readFileIntoVector(filename);
  return {buf1.data(), buf1.size()};
}

extern "C" char const GIT_COMMIT[];

int main(int argc, char **argv) {

  MainOpt MainOptions;

  fmt::print("send-command {:.7} (ESS, BrightnESS)\n"
             "  Contact: dominik.werder@psi.ch\n\n",
             GIT_COMMIT);

  CLI::App App{
      "Writes NeXus files in a format specified with a json template.\n"
      "Writer modules can be used to populate the file from Kafka topics.\n"};

  App.set_config(); // disable ini config file
  App.add_option("--teamid", MainOptions.teamid, "");
  App.add_option(
      "--cmd", MainOptions.cmd,
      "<command>\n"
      "                              Use a command file: file:<filename>\n"
      "                              Stop writing file-with-id and timestamp "
      "(optional): stop:<jobid>[:<timestamp>]\n"
      "                              Terminate the filewriter process: exit");
  std::string LogLevelInfoStr =
      R"*(Set log message level. Set to 0 - 5 or one of
  `Trace`, `Debug`, `Info`, `Warning`, `Error`
  or `Critical`. Ex: "-v Debug". Default: `Error`)*";
  App.add_option(
         "-v,--verbosity",
         [&MainOptions, LogLevelInfoStr](std::vector<std::string> Input) {
           return parseLogLevel(Input, MainOptions.LoggingLevel);
         },
         LogLevelInfoStr)
      ->set_default_str("Error");
  addUriOption(App, "--broker", MainOptions.broker,
               "<host[:port]/topic>\n"
               "                              Host, port, topic where the "
               "command should be sent to.",
               false);
  addUriOption(App, "--graylog-logger-address",
               MainOptions.GraylogLoggerAddress,
               "<host:port> Log to Graylog via graylog_logger library", false);
  App.add_option("--log-file", MainOptions.LogFilename,
                 "Specify file to log to");
  CLI11_PARSE(App, argc, argv);
  ::setUpLogging(MainOptions.LoggingLevel, "", MainOptions.LogFilename,
                 MainOptions.GraylogLoggerAddress);
  auto Logger = getLogger();
  MainOptions.BrokerSettings.Address = MainOptions.broker.HostPort;
  auto producer =
      std::make_shared<KafkaW::Producer>(MainOptions.BrokerSettings);
  KafkaW::ProducerTopic pt(producer, MainOptions.broker.Topic);
  if (MainOptions.cmd == "new") {
    auto NewCommandMsg =
        make_command(MainOptions.BrokerSettings.Address, MainOptions.teamid);
    Logger->trace("sending {}", NewCommandMsg);
    pt.produce(NewCommandMsg);
  } else if (MainOptions.cmd == "exit") {
    auto ExitCommandMsg = make_command_exit(MainOptions.BrokerSettings.Address,
                                            MainOptions.teamid);
    Logger->trace("sending {}", ExitCommandMsg);
    pt.produce(ExitCommandMsg);
  } else if (MainOptions.cmd.substr(0, 5) == "file:") {
    auto CommandMsgFromFile = make_command_from_file(MainOptions.cmd.substr(5));
    Logger->trace("sending:\n{}", CommandMsgFromFile);
    pt.produce(CommandMsgFromFile);
  } else if (MainOptions.cmd.substr(0, 5) == "stop:") {
    auto input = MainOptions.cmd.substr(5);
    std::chrono::milliseconds stop_time{0};
    std::string::size_type n{input.find(':')};
    std::string StopCommandMsg;
    if (n != std::string::npos) {
      auto result = strtoul(&input[n + 1], nullptr, 0);
      if (result) {
        stop_time = std::chrono::milliseconds{result};
      }
      StopCommandMsg = make_command_stop(MainOptions.BrokerSettings.Address,
                                         input.substr(0, n), stop_time);
    } else {
      StopCommandMsg =
          make_command_stop(MainOptions.BrokerSettings.Address, input);
    }
    Logger->trace("sending {}", StopCommandMsg);
    pt.produce(StopCommandMsg);
  }
  Logger->flush();
  return 0;
}
