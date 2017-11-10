#include <algorithm>
#include <cstdint>
#include <gtest/gtest.h>
#include <random>
#include <stdexcept>

#include <StatusWriter.hpp>
#include <StreamMaster.hpp>
#include <librdkafka/rdkafkacpp.h>

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

namespace stub {
class Streamer {
public:
  using Error = FileWriter::Status::StreamerErrorCode;
  using Options = std::vector<std::string>;

  Streamer() {}
  Streamer(const std::string &, const std::string &, const Options &,
           const Options &) {}
  const Error set_start_time(const milliseconds &) const {
    return Error::no_error;
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
