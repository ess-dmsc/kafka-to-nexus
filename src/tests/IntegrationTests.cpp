#include "CLIOptions.h"
#include "MainOpt.h"
#include "roundtrip.h"
#include <gtest/gtest.h>
#include <librdkafka/rdkafka.h>
#include <CLI/CLI.hpp>

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  CLI::App App{""};
  auto Options = std::unique_ptr<MainOpt>(new MainOpt());
  setCLIOptions(App, *Options);

  CLI11_PARSE(App, argc, argv);
  setup_logger_from_options(*Options);
  SetTestOptions(Options.get());
  
  return RUN_ALL_TESTS();
}

static_assert(RD_KAFKA_RESP_ERR_NO_ERROR == 0,
              "Make sure that NO_ERROR is and stays 0");
