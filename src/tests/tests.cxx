#include "../KafkaW.h"
#include "../MainOpt.h"
#include "helper.h"
#include "roundtrip.h"
#include <gtest/gtest.h>

#include <h5cpp/error/error.hpp>

int main(int argc, char **argv) {
  hdf5::error::Singleton::instance().auto_print(false);

  ::testing::InitGoogleTest(&argc, argv);
  std::string f = ::testing::GTEST_FLAG(filter);
  if (f.find("remote_kafka") == std::string::npos) {
    f = f + std::string(":-*remote_kafka*");
  }
  ::testing::GTEST_FLAG(filter) = f;

  auto po = parse_opt(argc, argv);
  if (po.first) {
    return 1;
  }
  auto opt = std::move(po.second);
  g_main_opt.store(opt.get());
  setup_logger_from_options(*opt);

  Roundtrip::opt = opt.get();

  auto gtest_result = RUN_ALL_TESTS();

  return gtest_result;
}

static_assert(RD_KAFKA_RESP_ERR_NO_ERROR == 0,
              "Make sure that NO_ERROR is and stays 0");
