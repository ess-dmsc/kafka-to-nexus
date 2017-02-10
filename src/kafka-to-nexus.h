#pragma once
#include "Master.h"

extern "C" char const GIT_COMMIT[];

// POD
struct MainOpt {
bool help = false;
bool verbose = false;
bool gtest = false;
BrightnESS::FileWriter::MasterConfig master_config;
};
