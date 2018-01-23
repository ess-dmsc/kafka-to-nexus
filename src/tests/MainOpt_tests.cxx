#include "../MainOpt.h"
#include "../logger.h"
#include <gtest/gtest.h>

using namespace rapidjson;

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
  std::pair<int, std::unique_ptr<MainOpt>> parse_opt_return =
      parse_opt(argv.size(), argv.data());
  ASSERT_EQ(parse_opt_return.first, 0);
  ASSERT_NE(parse_opt_return.second, nullptr);
  ASSERT_EQ(parse_opt_return.second->hdf_output_prefix, "/some/path");
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
  std::pair<int, std::unique_ptr<MainOpt>> parse_opt_return =
      parse_opt(argv.size(), argv.data());
  ASSERT_EQ(parse_opt_return.first, 0);
  auto &main_opt = parse_opt_return.second;
  ASSERT_NE(main_opt, nullptr);
  main_opt->parse_config_json(std::string(jsontxt.data(), jsontxt.size()));
  ASSERT_EQ(main_opt->hdf_output_prefix, "/some/directory");
}
