#pragma once
#include "../MainOpt.h"
#include <gtest/gtest.h>

class Roundtrip : public ::testing::Test {
public:
};

namespace FileWriter {
namespace Test {

void roundtrip_simple_01(MainOpt &opt);
}
} // namespace FileWriter
