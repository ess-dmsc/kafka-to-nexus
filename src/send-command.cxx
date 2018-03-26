#include "CLIOptions.h"
#include "KafkaW/KafkaW.h"
#include "helper.h"
#include "uri.h"

#include <CLI/CLI.hpp>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <string>

using uri::URI;

// POD
struct MainOpt {
  uint64_t teamid = 0;
  URI broker{"localhost:9092/commands"};
  KafkaW::BrokerSettings BrokerSettings;
  std::string cmd;
};

std::string make_command(const std::string &broker, const uint64_t &teamid) {
  using namespace rapidjson;
  Document d;
  auto &a = d.GetAllocator();
  d.SetObject();
  d.AddMember("cmd", Value("FileWriter_new", a), a);
  d.AddMember("teamid", teamid, a);
  d.AddMember("broker", Value(broker.c_str(), a), a);
  d.AddMember("filename",
              Value(fmt::format("tmp-{:016x}.h5", teamid).c_str(), a), a);
  Value sa;
  sa.SetArray();
  {
    Value st;
    st.SetObject();
    st.AddMember("broker", Value(broker.c_str(), a), a);
    st.AddMember("topic", Value("topic.with.multiple.sources", a), a);
    st.AddMember("source", Value("source-00", a), a);
    sa.PushBack(st, a);
  }
  d.AddMember("streams", sa, a);
  StringBuffer buf1;
  PrettyWriter<StringBuffer> wr(buf1);
  d.Accept(wr);
  return buf1.GetString();
}

std::string make_command_exit(const std::string &broker,
                              const uint64_t &teamid) {
  using namespace rapidjson;
  Document d;
  auto &a = d.GetAllocator();
  d.SetObject();
  d.AddMember("cmd", Value("FileWriter_exit", a), a);
  d.AddMember("teamid", teamid, a);
  StringBuffer buf1;
  PrettyWriter<StringBuffer> wr(buf1);
  d.Accept(wr);
  return buf1.GetString();
}

std::string make_command_stop(
    const std::string &broker, const std::string &job_id,
    const std::chrono::milliseconds &stop_time = std::chrono::milliseconds{0}) {
  using namespace rapidjson;
  Document d;
  auto &a = d.GetAllocator();
  d.SetObject();
  d.AddMember("cmd", Value("FileWriter_stop", a), a);
  d.AddMember("job_id", rapidjson::StringRef(job_id.c_str(), job_id.size()), a);
  if (stop_time.count()) {
    d.AddMember("stop_time", stop_time.count(), a);
  }
  StringBuffer buf1;
  PrettyWriter<StringBuffer> wr(buf1);
  d.Accept(wr);
  return buf1.GetString();
}

std::string make_command_from_file(const std::string &filename) {
  using namespace rapidjson;
  std::ifstream ifs(filename);
  if (!ifs.good()) {
    LOG(Sev::Warning, "can not open file {}", filename);
    return "";
  }
  LOG(Sev::Debug, "make_command_from_file {}", filename);
  auto buf1 = gulp(filename);
  return {buf1.data(), buf1.size()};
}

extern "C" char const GIT_COMMIT[];

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
  add_option(App, "--broker", opt.broker,
             "<//host[:port]/topic>\n"
             "                              Host, port, topic where the "
             "command should be sent to.",
             false);
  CLI11_PARSE(App, argc, argv);

  opt.BrokerSettings.Address = opt.broker.host_port;
  auto producer = std::make_shared<KafkaW::Producer>(opt.BrokerSettings);
  KafkaW::Producer::Topic pt(producer, opt.broker.topic);
  if (opt.cmd == "new") {
    auto m1 = make_command(opt.BrokerSettings.Address, opt.teamid);
    LOG(Sev::Debug, "sending {}", m1);
    pt.produce((uint8_t *)m1.data(), m1.size(), true);
  } else if (opt.cmd == "exit") {
    auto m1 = make_command_exit(opt.BrokerSettings.Address, opt.teamid);
    LOG(Sev::Debug, "sending {}", m1);
    pt.produce((uint8_t *)m1.data(), m1.size(), true);
  } else if (opt.cmd.substr(0, 5) == "file:") {
    auto m1 = make_command_from_file(opt.cmd.substr(5));
    LOG(Sev::Debug, "sending:\n{}", m1);
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
    LOG(Sev::Debug, "sending {}", m1);
    pt.produce((uint8_t *)m1.data(), m1.size(), true);
  }
  return 0;
}
