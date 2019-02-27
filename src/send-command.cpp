#include "CLIOptions.h"
#include "KafkaW/KafkaW.h"
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
  auto Logger = spdlog::get("filewriterlogger");
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

bool parseLogLevels(std::vector<std::string> LogLevelString,
                    spdlog::level::level_enum &LogLevelResult) {
  std::map<std::string, spdlog::level::level_enum> LevelMap{
      {"Critical", spdlog::level::critical}, {"Error", spdlog::level::err},
      {"Warning", spdlog::level::warn},      {"Info", spdlog::level::info},
      {"Debug", spdlog::level::debug},       {"Trace", spdlog::level::trace}};

  if (LogLevelString.size() != 1) {
    return false;
  }
  try {
    LogLevelResult = LevelMap.at(LogLevelString.at(0));
    return true;
  } catch (std::out_of_range &e) {
    // Do nothing
  }
  try {
    int TempLogMessageLevel = std::stoi(LogLevelString.at(0));
    if (TempLogMessageLevel < 1 or TempLogMessageLevel > 7) {
      return false;
    }
    LogLevelResult = spdlog::level::level_enum(TempLogMessageLevel);
  } catch (std::invalid_argument &e) {
    return false;
  }
  return true;
}

int main(int argc, char **argv) {

  MainOpt opt;

  fmt::print("send-command {:.7} (ESS, BrightnESS)\n"
             "  Contact: dominik.werder@psi.ch\n\n",
             GIT_COMMIT);

  CLI::App App{
      "Writes NeXus files in a format specified with a json template.\n"
      "Writer modules can be used to populate the file from Kafka topics.\n"};

  App.set_config(); // disable ini config file
  App.add_option("--teamid", opt.teamid, "");
  App.add_option(
      "--cmd", opt.cmd,
      "<command>\n"
      "                              Use a command file: file:<filename>\n"
      "                              Stop writing file-with-id and timestamp "
      "(optional): stop:<jobid>[:<timestamp>]\n"
      "                              Terminate the filewriter process: exit");
  std::string LogLevelInfoStr =
      R"*(Set log message level. Set to 1 - 7 or one of
  `Critical`, `Error`, `Warning`, `Notice`, `Info`,
  or `Debug`. Ex: "-l Notice")*";
  App.add_option("-v,--verbosity",
                 [&opt, LogLevelInfoStr](std::vector<std::string> Input) {
                   return parseLogLevels(Input, opt.LoggingLevel);
                 },
                 LogLevelInfoStr)
      ->set_default_val("Error");
  addOption(App, "--broker", opt.broker,
            "<//host[:port]/topic>\n"
            "                              Host, port, topic where the "
            "command should be sent to.",
            false);
  App.add_option("--log-file", opt.LogFilename, "Specify file to log to");
  CLI11_PARSE(App, argc, argv);
  ::setUpLogging(opt.LoggingLevel, "", opt.LogFilename, "");
  auto Logger = spdlog::get("filewriterlogger");
  opt.BrokerSettings.Address = opt.broker.HostPort;
  auto producer = std::make_shared<KafkaW::Producer>(opt.BrokerSettings);
  KafkaW::Producer::Topic pt(producer, opt.broker.Topic);
  if (opt.cmd == "new") {
    auto m1 = make_command(opt.BrokerSettings.Address, opt.teamid);
    Logger->trace("sending {}", m1);
    pt.produce((uint8_t *)m1.data(), m1.size(), true);
  } else if (opt.cmd == "exit") {
    auto m1 = make_command_exit(opt.BrokerSettings.Address, opt.teamid);
    Logger->trace("sending {}", m1);
    pt.produce((uint8_t *)m1.data(), m1.size(), true);
  } else if (opt.cmd.substr(0, 5) == "file:") {
    auto m1 = make_command_from_file(opt.cmd.substr(5));
    Logger->trace("sending:\n{}", m1);
    pt.produce((uint8_t *)m1.data(), m1.size(), true);
  } else if (opt.cmd.substr(0, 5) == "stop:") {
    auto input = opt.cmd.substr(5);
    std::chrono::milliseconds stop_time{0};
    std::string::size_type n{input.find(':')};
    std::string m1;
    if (n != std::string::npos) {
      auto result = strtoul(&input[n + 1], NULL, 0);
      if (result) {
        stop_time = std::chrono::milliseconds{result};
      }
      m1 = make_command_stop(opt.BrokerSettings.Address, input.substr(0, n),
                             stop_time);
    } else {
      m1 = make_command_stop(opt.BrokerSettings.Address, input);
    }
    Logger->trace("sending {}", m1);
    pt.produce((uint8_t *)m1.data(), m1.size(), true);
  }
  return 0;
}
