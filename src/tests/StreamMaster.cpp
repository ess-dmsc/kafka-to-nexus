#include <StatusWriter.h>
#include <StreamMaster.h>
#include <StreamerOptions.h>
#include <algorithm>
#include <cstdint>
#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>
#include <random>
#include <stdexcept>

class StubStreamer : public FileWriter::StreamerI {
public:
  using Error = FileWriter::Status::StreamerStatus;
  using Options = std::vector<std::string>;

  StubStreamer() {}
  StubStreamer(const std::string &, const std::string &, const Options &,
               const Options &) {}
  const Error set_start_time(const std::chrono::milliseconds &) const {
    return Error::OK;
  }
  int &n_sources() { return n_src; };

private:
  int n_src;
};

using StreamMaster = FileWriter::StreamMaster;

class StreamMaster_Test : public ::testing::Test {

protected:
  virtual void SetUp() {}

private:
  std::unique_ptr<StreamMaster> stream_master;
};

TEST(StreamMaster, empty_test) {
  StreamMaster("no-broker", std::make_unique<FileWriter::FileWriterTask>(
                                "service-id", nullptr),
               MainOpt(), nullptr);
}
