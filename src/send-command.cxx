#include "KafkaW.h"
#include "logger.h"
#include <cstdio>
#include <cstdlib>
#include <getopt.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <string>

#include "helper.h"
#include "uri.h"
#include "utils.h"

#include <fstream>
#include <iostream>

using uri::URI;
using ESSTimeStamp = FileWriter::ESSTimeStamp;

// POD
struct MainOpt {
  bool help = false;
  bool verbose = false;
  uint64_t teamid = 0;
  URI broker{"localhost:9092/commands"};
  KafkaW::BrokerOpt broker_opt;
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

std::string make_command_stop(const std::string &broker,
                              const std::string &job_id,
                              const ESSTimeStamp stop_time& = ESSTimeStamp{0}) {
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
    LOG(3, "can not open file {}", filename);
    return "";
  }
  LOG(4, "make_command_from_file {}", filename);
  auto buf1 = gulp(filename);
  return {buf1.data(), buf1.size()};
}

extern "C" char const GIT_COMMIT[];

int main(int argc, char **argv) {

  MainOpt opt;

  static struct option long_options[] = {
      {"help", no_argument, 0, 'h'},
      {"teamid", required_argument, 0, 0},
      {"cmd", required_argument, 0, 0},
      {"broker", required_argument, 0, 0},
      {0, 0, 0, 0},
  };
  int option_index = 0;
  bool getopt_error = false;
  while (true) {
    int c = getopt_long(argc, argv, "vh", long_options, &option_index);
    // LOG(2, "c getopt {}", c);
    if (c == -1)
      break;
    if (c == '?') {
      getopt_error = true;
    }
    switch (c) {
    case 'v':
      opt.verbose = true;
      log_level = std::min(9, log_level + 1);
      break;
    case 'h':
      opt.help = true;
      break;
    case 0:
      auto lname = long_options[option_index].name;
      if (std::string("broker") == lname) {
        opt.broker = URI(optarg);
      }
      if (std::string("help") == lname) {
        opt.help = true;
      }
      if (std::string("teamid") == lname) {
        opt.teamid = strtoul(optarg, nullptr, 0);
      }
      if (std::string("cmd") == lname) {
        opt.cmd = optarg;
      }
      break;
    }
  }

  if (getopt_error) {
    LOG(2, "ERROR parsing command line options");
    opt.help = true;
    return 1;
  }

  printf("send-command	%.7s\n", GIT_COMMIT);
  printf("	Contact: dominik.werder@psi.ch\n\n");

  if (opt.help) {
    printf("Send a command to kafka-to-nexus.\n"
           "\n"
           "kafka-to-nexus\n"
           "  --help, -h\n"
           "\n"
           "  --broker          <//host[:port]/topic>\n"
           "    Host, port, topic where the command should be sent to.\n"
           "\n"
           "  --cmd             <command>\n"
           "    Use a command file: file:<filename>\n"
           "    Stop writing file-with-id and timestamp (optional): "
           "stop:<jobid>[:<timestamp>]\n"
           "    Terminate the filewriter process: exit\n"
           "\n"
           "   -v\n"
           "    Increase verbosity\n"
           "\n");
    return 1;
  }

  opt.broker_opt.address = opt.broker.host_port;
  auto producer = std::make_shared<KafkaW::Producer>(opt.broker_opt);
  KafkaW::Producer::Topic pt(producer, opt.broker.topic);
  if (opt.cmd == "new") {
    auto m1 = make_command(opt.broker_opt.address, opt.teamid);
    LOG(4, "sending {}", m1);
    pt.produce((uint8_t *)m1.data(), m1.size(), true);
  } else if (opt.cmd == "exit") {
    auto m1 = make_command_exit(opt.broker_opt.address, opt.teamid);
    LOG(4, "sending {}", m1);
    pt.produce((uint8_t *)m1.data(), m1.size(), true);
  } else if (opt.cmd.substr(0, 5) == "file:") {
    auto m1 = make_command_from_file(opt.cmd.substr(5));
    LOG(4, "sending:\n{}", m1);
    pt.produce((uint8_t *)m1.data(), m1.size(), true);
  } else if (opt.cmd.substr(0, 5) == "stop:") {
    auto input = opt.cmd.substr(5);
    ESSTimeStamp stop_time{0};
    std::string::size_type n{input.find(':')};
    std::string m1;
    if (n != std::string::npos) {
      auto result = strtoul(&input[n + 1], NULL, 0);
      if (result) {
        stop_time = ESSTimeStamp(result);
      }
      m1 = make_command_stop(opt.broker_opt.address, input.substr(0, n),
                             stop_time);
    } else {
      m1 = make_command_stop(opt.broker_opt.address, input);
    }
    LOG(4, "sending {}", m1);
    pt.produce((uint8_t *)m1.data(), m1.size(), true);
  }
  return 0;
}
