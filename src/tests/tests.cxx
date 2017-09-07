#include "../KafkaW.h"
#include "../MainOpt.h"
#include "logpid.h"
#include "roundtrip.h"
#include <gtest/gtest.h>
#include <mpi.h>

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

  logpid("tmp-pid.txt");
  LOG(3, "sleep 5");
  // sleep_ms(5000);

  Roundtrip::opt = opt.get();

  MPI_Init(&argc, &argv);
  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  LOG(3, "Running tests with MPI as {} of {}", rank, size);

  auto gtest_result = RUN_ALL_TESTS();

  MPI_Finalize();

  return gtest_result;
}

static_assert(RD_KAFKA_RESP_ERR_NO_ERROR == 0,
              "Make sure that NO_ERROR is and stays 0");

#if 0
	if (false) {
		// test if log messages arrive on all destinations
		for (int i1 = 0; i1 < 100; ++i1) {
			LOG(i1 % 8, "Log ix {} level {}", i1, i1 % 8);
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		}
	}
#endif
