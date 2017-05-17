#pragma once

#include "Master.h"
#include <atomic>
#include <rapidjson/document.h>
#include <string>
#include <utility>

// POD
struct MainOpt {
  bool help = false;
  bool verbose = false;
  bool gtest = false;
  bool use_signal_handler = true;
  BrightnESS::FileWriter::MasterConfig master_config;
  std::string kafka_gelf;
  std::string graylog_logger_address;
  std::atomic<BrightnESS::FileWriter::Master *> master;
  rapidjson::Document config_file;
  int parse_config_file(std::string fname);
};

std::pair<int, std::unique_ptr<MainOpt>> parse_opt(int argc, char **argv);
void setup_logger_from_options(MainOpt const &opt);
extern std::atomic<MainOpt *> g_main_opt;
