#include "FileWriterTask.h"
#include "JobCreator.h"
#include "MetaData/Tracker.h"
#include "Metrics/Metric.h"
#include "logger.h"
#include <ep01_epics_connection_generated.h>
#include <ev44_events_generated.h>
#include <f144_logdata_generated.h>
#include <iostream>
#include <memory>
#include <utility>

using std::chrono_literals::operator""ms;

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
					"type": "string"
				}
			},
			{
				"module": "mdat",
				"config": {
					"name": "start_time"
				}
			},
			{
				"module": "mdat",
				"config": {
					"name": "end_time"
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
                                                                "topic": "local_motion",
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

class StubConsumer : public Kafka::ConsumerInterface {
public:
  explicit StubConsumer(std::shared_ptr<std::vector<FileWriter::Msg>> messages)
      : messages(std::move(messages)) {}
  ~StubConsumer() override = default;

  std::pair<Kafka::PollStatus, FileWriter::Msg> poll() override {
    if (offset < messages->size()) {
      auto temp = FileWriter::Msg{messages->at(offset).data(),
                                  messages->at(offset).size(),
                                  messages->at(offset).getMetaData()};
      ++offset;
      return {Kafka::PollStatus::Message, std::move(temp)};
    }
    return {Kafka::PollStatus::TimedOut, FileWriter::Msg()};
  };

  void addPartitionAtOffset([[maybe_unused]] std::string const &Topic,
                            [[maybe_unused]] int PartitionId,
                            [[maybe_unused]] int64_t Offset) override{};

  void addTopic([[maybe_unused]] std::string const &Topic) override {}

  void assignAllPartitions(
      [[maybe_unused]] std::string const &Topic,
      [[maybe_unused]] time_point const &StartTimestamp) override {}

  const RdKafka::TopicMetadata *
  getTopicMetadata([[maybe_unused]] const std::string &Topic,
                   [[maybe_unused]] RdKafka::Metadata *MetadataPtr) override {
    return nullptr;
  }

  size_t offset = 0;
  std::string topic;
  int32_t partition = 0;
  std::shared_ptr<std::vector<FileWriter::Msg>> messages;
};

class StubConsumerFactory : public Kafka::ConsumerFactoryInterface {
public:
  std::shared_ptr<Kafka::ConsumerInterface> createConsumer(
      [[maybe_unused]] Kafka::BrokerSettings const &settings) override {
    return {};
  }
  std::shared_ptr<Kafka::ConsumerInterface>
  createConsumerAtOffset([[maybe_unused]] Kafka::BrokerSettings const &settings,
                         std::string const &topic, int partition_id,
                         [[maybe_unused]] int64_t offset) override {
    auto consumer = std::make_shared<StubConsumer>(messages);
    consumer->topic = topic;
    consumer->partition = partition_id;
    return consumer;
  }
  ~StubConsumerFactory() override = default;

  // One set of messages shared by all topics/consumers.
  std::shared_ptr<std::vector<FileWriter::Msg>> messages =
      std::make_shared<std::vector<FileWriter::Msg>>();
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

void add_message(StubConsumerFactory *consumer_factory,
                 std::pair<std::unique_ptr<uint8_t[]>, size_t> flatbuffer,
                 std::chrono::milliseconds timestamp, int64_t offset,
                 int32_t partition) {
  FileWriter::MessageMetaData metadata;
  metadata.Timestamp = timestamp;
  metadata.Offset = offset;
  metadata.Partition = partition;
  consumer_factory->messages->emplace_back(flatbuffer.first.get(),
                                           flatbuffer.second, metadata);
}

int main([[maybe_unused]] int argc, [[maybe_unused]] char **argv) {
  using std::chrono_literals::operator""ms;
  std::cout << "Starting writing\n";

  std::unique_ptr<Metrics::IRegistrar> registrar =
      std::make_unique<FakeRegistrar>();
  auto tracker = std::make_shared<FakeTracker>();
  auto consumer_factory = std::make_shared<StubConsumerFactory>();
  auto metadata_enquirer = std::make_shared<StubMetadataEnquirer>();

  // Pre-populate kafka messages - time-stamps must be in order?
  int64_t offset = 0;
  auto msg = create_f144_message_double("delay:source:chopper", 100, 1000);
  add_message(consumer_factory.get(), std::move(msg), 1000ms, offset++, 0);

  msg = create_ep01_message_double("delay:source:chopper",
                                   ConnectionInfo::CONNECTED, 1001);
  add_message(consumer_factory.get(), std::move(msg), 1001ms, offset++, 0);

  msg = create_f144_message_double("delay:source:chopper", 101, 1100);
  add_message(consumer_factory.get(), std::move(msg), 1100ms, offset++, 0);

  msg = create_f144_message_array_double("speed:source:chopper",
                                         {1000, 1010, 1020}, 1200);
  add_message(consumer_factory.get(), std::move(msg), 1200ms, offset++, 0);

  msg = create_ep01_message_double("speed:source:chopper",
                                   ConnectionInfo::CONNECTED, 1201);
  add_message(consumer_factory.get(), std::move(msg), 1201ms, offset++, 0);

  msg = create_f144_message_array_double("speed:source:chopper",
                                         {2000, 2010, 2020}, 1250);
  add_message(consumer_factory.get(), std::move(msg), 1250ms, offset++, 0);

  msg = create_f144_message_double("delay:source:chopper", 102, 2100);
  add_message(consumer_factory.get(), std::move(msg), 2100ms, offset++, 0);

  Command::StartInfo start_info;
  start_info.NexusStructure = example_json;
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