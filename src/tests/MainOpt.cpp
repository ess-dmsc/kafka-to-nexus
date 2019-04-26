#include "MainOpt.h"
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
using nlohmann::json;

TEST(MainOpt, findAndAddCommandsAddsNoCommandsIfJsonIsEmpty) {
  MainOpt Mainopt;
  Mainopt.CommandsJson = json::parse("{}");
  Mainopt.findAndAddCommands();
  ASSERT_TRUE(Mainopt.CommandsFromJson.empty());
}

TEST(MainOpt, findAndAddCommandsAddsCommands) {
  MainOpt Mainopt;
  Mainopt.CommandsJson =
      json::parse(R"({"commands":[{"some command":"as discussed above"}]})");
  Mainopt.findAndAddCommands();
  ASSERT_EQ("{\"some command\":\"as discussed above\"}",
            Mainopt.CommandsFromJson.at(0));
}

TEST(MainOpt, findAndAddCommandsAddsMultipleCommands) {
  MainOpt Mainopt;
  Mainopt.CommandsJson = json::parse(
      R"({"commands":[{"some command":"as discussed above"},{"another command":"test"}]})");
  Mainopt.findAndAddCommands();
  ASSERT_EQ("{\"some command\":\"as discussed above\"}",
            Mainopt.CommandsFromJson.at(0));
  ASSERT_EQ("{\"another command\":\"test\"}", Mainopt.CommandsFromJson.at(1));
}
