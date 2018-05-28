#include <StatusWriter.h>
#include <StreamMaster.h>
#include <StreamerOptions.h>
#include <algorithm>
#include <cstdint>
#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>
#include <random>
#include <stdexcept>

namespace stub {
class Streamer {
public:
  using Error = FileWriter::Status::StreamerError;
  using Options = std::vector<std::string>;

  Streamer() {}
  Streamer(const std::string &, const std::string &, const Options &,
           const Options &) {}
  const Error set_start_time(const std::chrono::milliseconds &) const {
    return Error::OK();
  }
  int &n_sources() { return n_src; };

private:
  int n_src;
};
} // namespace stub

using StreamMaster = FileWriter::StreamMaster<stub::Streamer>;

class StreamMaster_Test : public ::testing::Test {

protected:
  virtual void SetUp() {}

private:
  std::unique_ptr<StreamMaster> stream_master;
};

// TEST_F(StreamMaster_Test, empty_test) {}
