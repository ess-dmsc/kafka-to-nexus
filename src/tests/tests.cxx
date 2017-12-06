#include "../KafkaW.h"
#include "../MainOpt.h"
#include "helper.h"
#include "logpid.h"
#include "roundtrip.h"
#include <gtest/gtest.h>
#if USE_PARALLEL_WRITER
#include <mpi.h>
#endif

int main(int argc, char **argv) {
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
  setup_logger_from_options(*opt);

  if (opt->logpid_sleep) {
    logpid("tmp-pid.txt");
    LOG(3, "sleep 5");
    sleep_ms(3000);
  }

  Roundtrip::opt = opt.get();

#if USE_PARALLEL_WRITER
  {
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    LOG(8, "Running tests with MPI as {} of {}", rank, size);
  }
#endif

  auto gtest_result = RUN_ALL_TESTS();

#if USE_PARALLEL_WRITER
  MPI_Finalize();
#endif

  return gtest_result;
}

static_assert(RD_KAFKA_RESP_ERR_NO_ERROR == 0,
              "Make sure that NO_ERROR is and stays 0");
