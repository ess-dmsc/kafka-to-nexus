#include "../MainOpt.h"
#include "CLIOptions.h"
#include <CLI/CLI.hpp>
#include <gtest/gtest.h>

std::vector<char> vector_from_literal(char const *literal) {
  std::vector<char> ret(literal, literal + strlen(literal) + 1);
  return ret;
}

TEST(MainOpt, parse_hdf_output_prefix_from_command_line) {
  std::vector<std::string> arg_strings = {"/some/program/name",
                                          "--hdf-output-prefix", "/some/path"};
  std::vector<std::vector<char>> args;
  std::vector<char *> argv;
  for (auto &x : arg_strings) {
    args.push_back(vector_from_literal(x.data()));
    argv.push_back(args.back().data());
  }

  CLI::App App{""};
  auto Options = std::unique_ptr<MainOpt>(new MainOpt());
  Options->init();
  setCLIOptions(App, *Options);
  App.parse(static_cast<int>(argv.size()), argv.data());

  ASSERT_EQ(Options->hdf_output_prefix, "/some/path");
}

TEST(MainOpt, parse_hdf_output_prefix_from_json_file) {
  std::string jsontxt(R""({"hdf-output-prefix": "/some/directory"})"");
  std::vector<std::string> arg_strings = {"/some/program/name"};
  std::vector<std::vector<char>> args;
  std::vector<char *> argv;
  for (auto &x : arg_strings) {
    args.push_back(vector_from_literal(x.data()));
    argv.push_back(args.back().data());
  }

  CLI::App App{""};
  auto Options = std::unique_ptr<MainOpt>(new MainOpt());
  Options->init();
  setCLIOptions(App, *Options);
  App.parse(static_cast<int>(argv.size()), argv.data());
  Options->parse_config_file();
  Options->parse_config_json(std::string(jsontxt.data(), jsontxt.size()));

  ASSERT_EQ(Options->hdf_output_prefix, "/some/directory");
}
