#pragma once

#include <atomic>
#include <utility>
#include <string>
#include "Master.h"

// POD
struct MainOpt {
bool help = false;
bool verbose = false;
bool gtest = false;
BrightnESS::FileWriter::MasterConfig master_config;
std::string kafka_gelf;
std::string graylog_logger_address;
std::atomic<Master *> master;
};

std::pair<int, std::unique_ptr<MainOpt>> parse_opt(int argc, char ** argv);
void setup_logger_from_options(MainOpt const & opt);
extern std::atomic<MainOpt *> g_main_opt;
