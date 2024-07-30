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

std::string const example_json = R"(
{
	"children": [{
		"name": "entry",
		"type": "group",
		"attributes": [{
			"name": "NX_class",
			"dtype": "string",
			"values": "NXentry"
		}],
		"children": [{
				"module": "dataset",
				"config": {
					"name": "title",
					"values": "This is a title",
					"dtype": "string"
				}
			},
			{
				"module": "mdat",
				"config": {
					"items": ["start_time", "end_time"]
				}
			},
			{
				"name": "instrument",
				"type": "group",
				"attributes": [{
					"name": "NX_class",
					"dtype": "string",
					"values": "NXinstrument"
				}],
				"children": [{
					"name": "mini_chopper",
					"type": "group",
					"attributes": [{
						"name": "NX_class",
						"dtype": "string",
						"values": "NXdisk_chopper"
					}],
					"children": [{
						"name": "delay",
						"type": "group",
						"attributes": [{
							"name": "NX_class",
							"dtype": "string",
							"values": "NXlog"
						}],
						"children": [{
							"module": "f144",
							"config": {
								"source": "delay:source:chopper",
								"topic": "local_choppers",
								"dtype": "double",
                                                                "value_units": "ns"
							}
						}]
					},{
                                                "name": "speed",
                                                "type": "group",
                                                "attributes": [{
                                                        "name": "NX_class",
                                                        "dtype": "string",
                                                        "values": "NXlog"
                                                }],
                                                "children": [{
                                                        "module": "f144",
                                                        "config": {
                                                                "source": "speed:source:chopper",
                                                                "topic": "local_choppers",
                                                                "dtype": "double",
                                                                "value_units": "Hz"
                                                        }
                                                }]
                                                }]
				}]
			}
		]
	}]
}
                             )";

class FakeRegistrar : public Metrics::IRegistrar {
public:
  void registerMetric([[maybe_unused]] Metrics::Metric &NewMetric,
                      [[maybe_unused]] std::vector<Metrics::LogTo> const
                          &SinkTypes) const override {}

  [[nodiscard]] std::unique_ptr<Metrics::IRegistrar> getNewRegistrar(
      [[maybe_unused]] std::string const &MetricsPrefix) const override {
    return std::make_unique<FakeRegistrar>();
  }
};

class FakeTracker : public MetaData::ITracker {
public:
  void
  registerMetaData([[maybe_unused]] MetaData::ValueBase NewMetaData) override {}
  void clearMetaData() override {}
  void
  writeToJSONDict([[maybe_unused]] nlohmann::json &JSONNode) const override {}
  void
  writeToHDF5File([[maybe_unused]] hdf5::node::Group &RootNode) const override {
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
    // TODO: populate this list at runtime
    return {"local_choppers", "local_motion"};
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
  app.add_option("-f, --file", json_file, "The JSON file to load");
  CLI11_PARSE(app, argc, argv);

  std::cout << "Starting writing\n";

  std::unique_ptr<Metrics::IRegistrar> registrar =
      std::make_unique<FakeRegistrar>();
  auto tracker = std::make_shared<FakeTracker>();
  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  auto metadata_enquirer = std::make_shared<StubMetadataEnquirer>();

  // Pre-populate kafka messages - time-stamps must be in order?
  int64_t offset = 0;
  auto msg = create_f144_message_double("delay:source:chopper", 100, 1000);
  add_message(*consumer_factory, std::move(msg), 1000ms, "local_choppers",
              offset++, 0);

  msg = create_ep01_message_double("delay:source:chopper",
                                   ConnectionInfo::CONNECTED, 1001);
  add_message(*consumer_factory, std::move(msg), 1001ms, "local_choppers",
              offset++, 0);

  msg = create_f144_message_double("delay:source:chopper", 101, 1100);
  add_message(*consumer_factory, std::move(msg), 1100ms, "local_choppers",
              offset++, 0);

  msg = create_f144_message_double("speed:source:chopper", 1000, 1200);
  add_message(*consumer_factory, std::move(msg), 1200ms, "local_choppers",
              offset++, 0);

  msg = create_ep01_message_double("speed:source:chopper",
                                   ConnectionInfo::CONNECTED, 1201);
  add_message(*consumer_factory, std::move(msg), 1201ms, "local_choppers",
              offset++, 0);

  msg = create_f144_message_double("speed:source:chopper", 2000, 1250);
  add_message(*consumer_factory, std::move(msg), 1250ms, "local_choppers",
              offset++, 0);

  msg = create_f144_message_double("delay:source:chopper", 102, 2100);
  add_message(*consumer_factory, std::move(msg), 2100ms, "local_choppers",
              offset++, 0);

  Command::StartMessage start_info;
  if (!json_file.empty()) {
    start_info.NexusStructure = readJsonFromFile(json_file);
  } else {
    start_info.NexusStructure = example_json;
  }
  start_info.JobID = "some_job_id";

  FileWriter::StreamerOptions streamer_options;
  streamer_options.StartTimestamp = time_point{0ms};
  streamer_options.StopTimestamp = time_point{1250ms};
  std::filesystem::path filepath{"../../example.hdf"};

  auto stream_controller = FileWriter::createFileWritingJob(
      start_info, streamer_options, filepath, registrar.get(), tracker,
      metadata_enquirer, consumer_factory);
  stream_controller->start();

  while (!stream_controller->isDoneWriting()) {
    std::cout << "Stream controller is writing\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  std::cout << "Stream controller has finished writing\n";

  return 0;
}
