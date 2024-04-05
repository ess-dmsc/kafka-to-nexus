#include "FileWriterTask.h"
#include "JobCreator.h"
#include "MetaData/Tracker.h"
#include "Metrics/Metric.h"
#include "logger.h"
#include <f144_logdata_generated.h>
#include <fmt/format.h>
#include <iostream>
#include <memory>

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
								"topic": "local_motion",
								"dtype": "double"
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
  writeToHDF5File([[maybe_unused]] hdf5::node::Group RootNode) const override {}
};

class StubConsumer : public Kafka::ConsumerInterface {
public:
  ~StubConsumer() override = default;

  std::pair<Kafka::PollStatus, FileWriter::Msg> poll() override {
    if (offset < messages.size()) {
      return {Kafka::PollStatus::Message, std::move(messages[offset++])};
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
  std::vector<FileWriter::Msg> messages;
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
    auto consumer = std::make_shared<StubConsumer>();
    consumer->topic = topic;
    consumer->partition = partition_id;
    consumers.emplace_back(topic, consumer);
    return consumer;
  }
  ~StubConsumerFactory() override = default;
  std::vector<std::tuple<std::string, std::shared_ptr<StubConsumer>>> consumers;
};

class StubMetadataEnquirer : public Kafka::MetadataEnquirer {
public:
  ~StubMetadataEnquirer() override = default;
  std::vector<std::pair<int, int64_t>> getOffsetForTime(
      [[maybe_unused]] std::string const &Broker,
      [[maybe_unused]] std::string const &Topic,
      [[maybe_unused]] std::vector<int> const &Partitions,
      [[maybe_unused]] time_point Time, [[maybe_unused]] duration TimeOut,
      [[maybe_unused]] Kafka::BrokerSettings BrokerSettings) override {
    return {{0, 0}};
  };

  std::vector<int> getPartitionsForTopic(
      [[maybe_unused]] std::string const &Broker,
      [[maybe_unused]] std::string const &Topic,
      [[maybe_unused]] duration TimeOut,
      [[maybe_unused]] Kafka::BrokerSettings BrokerSettings) override {
    return {0};
  }

  std::set<std::string>
  getTopicList([[maybe_unused]] std::string const &Broker,
               [[maybe_unused]] duration TimeOut,
               [[maybe_unused]] Kafka::BrokerSettings BrokerSettings) override {
    return {"local_motion"};
  }
};

std::pair<std::unique_ptr<uint8_t[]>, size_t>
create_f144_message_double(std::string const &source, double value,
                           int64_t timestamp) {
  auto builder = flatbuffers::FlatBufferBuilder();
  auto source_name_offset = builder.CreateString(source);
  auto value_offset = CreateDouble(builder, value).Union();

  f144_LogDataBuilder f144_builder(builder);
  f144_builder.add_value(value_offset);
  f144_builder.add_source_name(source_name_offset);
  f144_builder.add_timestamp(timestamp);
  f144_builder.add_value_type(Value::Double);
  Finishf144_LogDataBuffer(builder, f144_builder.Finish());

  size_t buffer_size = builder.GetSize();
  auto buffer = std::make_unique<uint8_t[]>(buffer_size);
  std::memcpy(buffer.get(), builder.GetBufferPointer(), buffer_size);
  return {std::move(buffer), buffer_size};
}

void add_message(StubConsumer *consumer,
                 std::pair<std::unique_ptr<uint8_t[]>, size_t> flatbuffer,
                 std::chrono::milliseconds timestamp, int64_t offset,
                 int32_t partition) {
  FileWriter::MessageMetaData metadata;
  metadata.Timestamp = timestamp;
  metadata.Offset = offset;
  metadata.Partition = partition;
  FileWriter::Msg message{flatbuffer.first.get(), flatbuffer.second, metadata};
  consumer->messages.push_back(std::move(message));
}

int main([[maybe_unused]] int argc, [[maybe_unused]] char **argv) {
  std::cout << "hello from the maker app\n";

  std::unique_ptr<Metrics::IRegistrar> registrar =
      std::make_unique<FakeRegistrar>();
  auto tracker = std::make_shared<FakeTracker>();
  auto consumer_factory = std::make_shared<StubConsumerFactory>();
  auto metadata_enquirer = std::make_shared<StubMetadataEnquirer>();

  Command::StartInfo start_info;
  start_info.NexusStructure = example_json;
  start_info.JobID = "some_job_id";

  FileWriter::StreamerOptions streamer_options;
  std::filesystem::path filepath{"example.hdf"};

  auto stream_controller = FileWriter::createFileWritingJob(
      start_info, streamer_options, filepath, registrar.get(), tracker,
      metadata_enquirer, consumer_factory);

  int a = 0;
  std::cin >> a;

  // Now add some messages
  auto msg = create_f144_message_double("delay:source:chopper", 123, 1000);

  auto &[t, c] = consumer_factory->consumers[0];

  add_message(c.get(), std::move(msg), 1000ms, 0, 0);

  msg = create_f144_message_double("delay:source:chopper", 456, 1100);
  add_message(c.get(), std::move(msg), 1100ms, 1, 0);

  std::cin >> a;

  return 0;
}
