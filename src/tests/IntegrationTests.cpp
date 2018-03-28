#include "MainOpt.h"
#include "roundtrip.h"
#include <gtest/gtest.h>
#include <librdkafka/rdkafka.h>

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  auto po = parse_opt(argc, argv);
  if (po.first) {
    return 1;
  }
  auto opt = std::move(po.second);
  setup_logger_from_options(*opt);
  SetTestOptions(opt.get());
  return RUN_ALL_TESTS();
}

static_assert(RD_KAFKA_RESP_ERR_NO_ERROR == 0,
              "Make sure that NO_ERROR is and stays 0");
