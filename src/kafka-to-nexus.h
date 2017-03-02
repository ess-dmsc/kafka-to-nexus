#pragma once
#include <atomic>
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
