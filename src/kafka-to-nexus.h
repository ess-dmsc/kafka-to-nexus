#pragma once
#include <atomic>
#include <utility>
#include <string>
#include "Master.h"

extern "C" char const GIT_COMMIT[];

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
