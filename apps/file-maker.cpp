#include "FileWriterTask.h"
#include "JobCreator.h"
#include "MetaData/Tracker.h"
#include "Metrics/Metric.h"
#include "logger.h"
#include <CLI/CLI.hpp>
#include <da00_dataarray_generated.h>
#include <ep01_epics_connection_generated.h>
#include <ev44_events_generated.h>
#include <f144_logdata_generated.h>
#include <iostream>
#include <memory>
#include <utility>

using std::chrono_literals::operator""ms;

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

std::pair<std::unique_ptr<uint8_t[]>, size_t>
create_f144_message_double(std::string const &source, double value,
                           int64_t timestamp_ms) {
  auto builder = flatbuffers::FlatBufferBuilder();
  auto source_name_offset = builder.CreateString(source);
  auto value_offset = CreateDouble(builder, value).Union();

  f144_LogDataBuilder f144_builder(builder);
  f144_builder.add_value(value_offset);
  f144_builder.add_source_name(source_name_offset);
  f144_builder.add_timestamp(timestamp_ms * 1000000);
  f144_builder.add_value_type(Value::Double);
  Finishf144_LogDataBuffer(builder, f144_builder.Finish());

  size_t buffer_size = builder.GetSize();
  auto buffer = std::make_unique<uint8_t[]>(buffer_size);
  std::memcpy(buffer.get(), builder.GetBufferPointer(), buffer_size);
  return {std::move(buffer), buffer_size};
}

std::pair<std::unique_ptr<uint8_t[]>, size_t>
create_f144_message_array_double(std::string const &source,
                                 const std::vector<double> &values,
                                 int64_t timestamp_ms) {
  auto builder = flatbuffers::FlatBufferBuilder();
  auto source_name_offset = builder.CreateString(source);
  auto values_offset = CreateArrayDoubleDirect(builder, &values).Union();

  f144_LogDataBuilder f144_builder(builder);
  f144_builder.add_value(values_offset);
  f144_builder.add_source_name(source_name_offset);
  f144_builder.add_timestamp(timestamp_ms * 1000000);
  f144_builder.add_value_type(Value::ArrayDouble);
  Finishf144_LogDataBuffer(builder, f144_builder.Finish());

  size_t buffer_size = builder.GetSize();
  auto buffer = std::make_unique<uint8_t[]>(buffer_size);
  std::memcpy(buffer.get(), builder.GetBufferPointer(), buffer_size);
  return {std::move(buffer), buffer_size};
}

std::pair<std::unique_ptr<uint8_t[]>, size_t>
create_ep01_message_double(std::string const &source, ConnectionInfo status,
                           int64_t timestamp_ms) {
  auto builder = flatbuffers::FlatBufferBuilder();
  auto source_name_offset = builder.CreateString(source);

  EpicsPVConnectionInfoBuilder ep01_builder(builder);
  ep01_builder.add_source_name(source_name_offset);
  ep01_builder.add_timestamp(timestamp_ms);
  ep01_builder.add_status(status);
  FinishEpicsPVConnectionInfoBuffer(builder, ep01_builder.Finish());

  size_t buffer_size = builder.GetSize();
  auto buffer = std::make_unique<uint8_t[]>(buffer_size);
  std::memcpy(buffer.get(), builder.GetBufferPointer(), buffer_size);
  return {std::move(buffer), buffer_size};
}

std::pair<std::unique_ptr<uint8_t[]>, size_t>
create_ev44_message(std::string const &source, int64_t message_id,
                    int64_t timestamp_ns,
                    std::vector<int32_t> const &time_of_flight,
                    std::vector<int32_t> const &pixel_ids) {
  auto builder = flatbuffers::FlatBufferBuilder();
  auto source_name_offset = builder.CreateString(source);

  std::vector<int64_t> reference_times = {timestamp_ns};
  auto reference_time_offset = builder.CreateVector(reference_times);
  std::vector<int32_t> reference_index = {0};
  auto reference_index_offset = builder.CreateVector(reference_index);

  auto time_of_flight_offset = builder.CreateVector(time_of_flight);
  auto pixel_ids_offset = builder.CreateVector(pixel_ids);

  Event44MessageBuilder ev44_builder(builder);
  ev44_builder.add_source_name(source_name_offset);
  ev44_builder.add_message_id(message_id);
  ev44_builder.add_reference_time(reference_time_offset);
  ev44_builder.add_reference_time_index(reference_index_offset);
  ev44_builder.add_time_of_flight(time_of_flight_offset);
  ev44_builder.add_pixel_id(pixel_ids_offset);
  FinishEvent44MessageBuffer(builder, ev44_builder.Finish());

  size_t buffer_size = builder.GetSize();
  auto buffer = std::make_unique<uint8_t[]>(buffer_size);
  std::memcpy(buffer.get(), builder.GetBufferPointer(), buffer_size);
  return {std::move(buffer), buffer_size};
}

std::pair<std::unique_ptr<uint8_t[]>, size_t>
create_da00_message_int32s(std::string const &source, int64_t timestamp_ms,
                           const std::vector<int32_t> &data) {
  auto builder = flatbuffers::FlatBufferBuilder();
  builder.ForceDefaults(true);

  auto source_name_offset = builder.CreateString(source);
  auto var_name_offset = builder.CreateString("value");

  auto var_axis = builder.CreateString("x");
  std::vector<flatbuffers::Offset<flatbuffers::String>> var_axes_offset = {
      var_axis};
  auto var_axes = builder.CreateVector(var_axes_offset);

  std::vector<int64_t> var_shape = {static_cast<int64_t>(data.size())};
  auto var_shape_offset = builder.CreateVector(var_shape);

  std::uint8_t *p_data;
  auto payload =
      builder.CreateUninitializedVector(data.size(), sizeof(data[0]), &p_data);
  std::memcpy(p_data, data.data(), sizeof(data[0]) * data.size());

  auto variable_offset =
      Createda00_Variable(builder, var_name_offset, 0, 0, 0, da00_dtype::int32,
                          var_axes, var_shape_offset, payload);
  std::vector<flatbuffers::Offset<da00_Variable>> variable_offsets = {
      variable_offset};
  auto variables = builder.CreateVector(variable_offsets);

  auto da00 = Createda00_DataArray(builder, source_name_offset, timestamp_ms,
                                   variables);
  builder.Finish(da00, "da00");

  auto verifier =
      flatbuffers::Verifier(builder.GetBufferPointer(), builder.GetSize());
  if (!Verifyda00_DataArrayBuffer(verifier)) {
    throw std::runtime_error("could not verify da00");
  }

  size_t buffer_size = builder.GetSize();
  auto buffer = std::make_unique<uint8_t[]>(buffer_size);
  std::memcpy(buffer.get(), builder.GetBufferPointer(), buffer_size);
  return {std::move(buffer), buffer_size};
}

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

  Command::StartInfo start_info;
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
