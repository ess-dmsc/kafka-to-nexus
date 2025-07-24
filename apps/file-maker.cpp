#include "FileWriterTask.h"
#include "FlatBufferGenerators.h"
#include "JobCreator.h"
#include "MetaData/Tracker.h"
#include "Metrics/Metric.h"
#include "logger.h"
#include <CLI/CLI.hpp>
#include <iostream>
#include <memory>
#include <utility>

using std::chrono_literals::operator""ms;
using namespace FlatBuffers;

std::string readJsonFromFile(const std::string &filePath) {
  std::ifstream file(filePath);
  if (!file.is_open()) {
    throw std::runtime_error("Unable to open file: " + filePath);
  }
  std::stringstream buffer;
  buffer << file.rdbuf();
  file.close();
  return buffer.str();
}

class FakeRegistrar : public Metrics::IRegistrar {
public:
  void
  registerMetric([[maybe_unused]] std::shared_ptr<Metrics::Metric> NewMetric,
                 [[maybe_unused]] std::vector<Metrics::LogTo> const &SinkTypes)
      const override {}

  [[nodiscard]] std::unique_ptr<Metrics::IRegistrar> getNewRegistrar(
      [[maybe_unused]] std::string const &MetricsPrefix) const override {
    return std::make_unique<FakeRegistrar>();
  }
};

class StubMetadataEnquirer : public Kafka::MetadataEnquirer {
public:
  ~StubMetadataEnquirer() override = default;
  std::vector<std::pair<int, int64_t>> getOffsetForTime(
      [[maybe_unused]] std::string const &Broker,
      [[maybe_unused]] std::string const &Topic,
      [[maybe_unused]] std::vector<int> const &Partitions,
      [[maybe_unused]] time_point Time, [[maybe_unused]] duration TimeOut,
      [[maybe_unused]] Kafka::BrokerSettings const &BrokerSettings) override {
    return {{0, 0}};
  };

  std::vector<int> getPartitionsForTopic(
      [[maybe_unused]] std::string const &Broker,
      [[maybe_unused]] std::string const &Topic,
      [[maybe_unused]] duration TimeOut,
      [[maybe_unused]] Kafka::BrokerSettings const &BrokerSettings) override {
    return {0};
  }

  std::set<std::string> getTopicList(
      [[maybe_unused]] std::string const &Broker,
      [[maybe_unused]] duration TimeOut,
      [[maybe_unused]] Kafka::BrokerSettings const &BrokerSettings) override {
    // NOTE: template files need to use these topics
    return {"local_choppers", "local_motion", "local_detector",
            "local_nicos_devices", "local_sample_env"};
  }
};

void add_message(Kafka::StubConsumerFactory &consumer_factory,
                 std::pair<std::unique_ptr<uint8_t[]>, size_t> flatbuffer,
                 std::chrono::milliseconds timestamp, std::string const &topic,
                 int64_t offset, int32_t partition) {
  FileWriter::MessageMetaData metadata;
  metadata.Timestamp = timestamp;
  metadata.Offset = offset;
  metadata.Partition = partition;
  metadata.topic = topic;
  consumer_factory.messages->emplace_back(flatbuffer.first.get(),
                                          flatbuffer.second, metadata);
}

int main([[maybe_unused]] int argc, [[maybe_unused]] char **argv) {
  CLI::App app{"file-maker app"};
  std::string json_file;
  app.add_option("-f, --file", json_file, "The JSON file to load")->required();
  std::string output_file;
  app.add_option("-o, --output-file", output_file,
                 "The name of the file to write")
      ->required();
  std::string data_file;
  app.add_option("-d, --data-file", data_file,
                 "The name of the file containing the data to be converted to "
                 "flatbuffers");
  std::string instrument_name;
  app.add_option("-i, --instrument", instrument_name, "The instrument name");
  CLI11_PARSE(app, argc, argv);

  std::cout << "Starting writing\n";

  std::unique_ptr<Metrics::IRegistrar> registrar =
      std::make_unique<FakeRegistrar>();
  auto tracker = std::make_shared<MetaData::Tracker>();
  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  auto metadata_enquirer = std::make_shared<StubMetadataEnquirer>();

  // Pre-populate kafka messages - time-stamps must be in order?
  int64_t offset = 0;

  if (!data_file.empty()) {
    auto flatbuffer_json = nlohmann::json::parse(readJsonFromFile(data_file));

    for (nlohmann::json::iterator it = flatbuffer_json.begin();
         it != flatbuffer_json.end(); ++it) {
      auto msg = FlatBuffers::convert_to_raw_flatbuffer(*it);
      auto timestamp_ms = std::chrono::milliseconds((*it)["kafka_timestamp"]);
      std::string topic = (*it)["topic"];
      add_message(*consumer_factory, std::move(msg), timestamp_ms, topic,
                  offset++, 0);
    }
  }

  Command::StartMessage start_info;
  start_info.NexusStructure = readJsonFromFile(json_file);
  std::filesystem::path template_path;
  if (!instrument_name.empty()) {
    start_info.InstrumentName = instrument_name;
    template_path = fmt::format("../../nexus/{0}/{0}.hdf", instrument_name);
  }
  start_info.JobID = "some_job_id";

  FileWriter::StreamerOptions streamer_options;
  streamer_options.StartTimestamp = time_point{10000ms};
  streamer_options.StopTimestamp = time_point{15000ms};
  std::filesystem::path filepath{output_file};

  auto stream_controller = FileWriter::createFileWritingJob(
      start_info, streamer_options, filepath, registrar.get(), tracker,
      template_path, metadata_enquirer, consumer_factory);
  stream_controller->start();

  while (!stream_controller->isDoneWriting()) {
    std::cout << "Stream controller is writing\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  std::cout << "Stream controller has finished writing\n";

  return 0;
}
