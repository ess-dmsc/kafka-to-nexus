#include "CommandHandler.h"
#include "helper.h"
#include "json.h"
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>

using nlohmann::json;
using namespace FileWriter;

TEST(CommandHandler_Test, TestFoundBrokerMatchesExpected) {
  auto CommandVector =
      gulp(std::string(TEST_DATA_PATH) + "/msg-cmd-new-01.json");
  ASSERT_EQ(FileWriter::findBroker(
                std::string(CommandVector.data(), CommandVector.size())),
            "localhost:9092");
}

TEST(CommandHandler_Test, TestMissingBrokerSetToDefault) {
  auto CommandVector =
      gulp(std::string(TEST_DATA_PATH) + "/msg-cmd-new-00.json");
  ASSERT_EQ(FileWriter::findBroker(
                std::string(CommandVector.data(), CommandVector.size())),
            "192.168.10.11:9092");
}
