#include "FileWriterTask.h"
#include "JobCreator.h"
#include "MetaData/Tracker.h"
#include "Metrics/Metric.h"
#include "logger.h"
#include <f144_logdata_generated.h>
#include <fmt/format.h>
#include <iostream>
#include <memory>

std::string example_json = R"(
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

namespace {
#define UNUSED_ARG(x) (void)x;
} // namespace

class FakeRegistrar : public Metrics::IRegistrar {
public:
  void
  registerMetric(Metrics::Metric &NewMetric,
                 std::vector<Metrics::LogTo> const &SinkTypes) const override {
    UNUSED_ARG(NewMetric);
    UNUSED_ARG(SinkTypes);
  }
};

class FakeTracker : public MetaData::ITracker {
public:
  void registerMetaData(MetaData::ValueBase NewMetaData) override {
    UNUSED_ARG(NewMetaData);
  }
  void clearMetaData() override {}
  void writeToJSONDict(nlohmann::json &JSONNode) const override {
    UNUSED_ARG(JSONNode);
  }
  void writeToHDF5File(hdf5::node::Group RootNode) const override {
    UNUSED_ARG(RootNode);
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

void send_f144_data_to_source(FileWriter::FileWriterTask &fw_task,
                              std::string const &source_name, double value,
                              int64_t timestamp) {
  auto [message, message_size] =
      create_f144_message_double(source_name, value, timestamp);
  FileWriter::FlatbufferMessage flatbuffer{message.get(), message_size};

  auto &sources = fw_task.sources();

  for (auto &source : sources) {
    if (source.sourcename() == "delay:source:chopper" &&
        source.writerModuleID() == "f144") {
      source.getWriterPtr()->write(flatbuffer);
    }
  }
}

int main(int argc, char **argv) {
  UNUSED_ARG(argc);
  UNUSED_ARG(argv);
  std::cout << "hello from the maker app\n";

  FakeRegistrar registrar;
  auto tracker = std::make_shared<FakeTracker>();

  FileWriter::FileWriterTask fw_task{&registrar, tracker};
  fw_task.setFullFilePath("", "example.hdf");

  std::vector<ModuleHDFInfo> module_info;
  fw_task.InitialiseHdf(example_json, module_info);

  // TODO: this duplicated code in JobCreator, can we remove this duplication?
  std::vector<ModuleSettings> module_settings =
      FileWriter::extractModuleInformationFromJson(module_info);

  std::vector<ModuleSettings> stream_settings;
  std::vector<ModuleSettings> link_settings;

  for (auto &Item : module_settings) {
    if (Item.isLink) {
      link_settings.push_back(std::move(Item));
    } else {
      stream_settings.push_back(std::move(Item));
    }
  }

  for (size_t i = 0; i < stream_settings.size(); ++i) {
    auto &Item = stream_settings[i];
    auto StreamGroup = hdf5::node::get_group(
        fw_task.hdfGroup(), Item.ModuleHDFInfoObj.HDFParentName);
    std::vector<ModuleSettings> TemporaryStreamSettings;
    try {
      Item.WriterModule = FileWriter::generateWriterInstance(Item);
      for (auto const &ExtraModule :
           Item.WriterModule->getEnabledExtraModules()) {
        auto ItemCopy = Item.getCopyForExtraModule();
        ItemCopy.Module = ExtraModule;
        TemporaryStreamSettings.push_back(std::move(ItemCopy));
      }
      FileWriter::setWriterHDFAttributes(StreamGroup, Item);
      Item.WriterModule->init_hdf(StreamGroup);
    } catch (std::runtime_error const &E) {
      auto ErrorMsg = fmt::format(
          R"(Could not initialise stream at path "{}" with configuration JSON "{}". Error was: {})",
          Item.ModuleHDFInfoObj.HDFParentName,
          Item.ModuleHDFInfoObj.ConfigStream, E.what());
      std::throw_with_nested(std::runtime_error(ErrorMsg));
    }
    try {
      Item.WriterModule->register_meta_data(StreamGroup, tracker);
    } catch (std::exception const &E) {
      std::throw_with_nested(std::runtime_error(fmt::format(
          R"(Exception encountered in WriterModule::Base::register_meta_data(). Module: "{}" Source: "{}"  Error message: {})",
          Item.Module, Item.Source, E.what())));
    }
    std::transform(TemporaryStreamSettings.begin(),
                   TemporaryStreamSettings.end(),
                   std::back_inserter(stream_settings),
                   [](auto &Element) { return std::move(Element); });
  }

  fw_task.writeLinks(link_settings);
  fw_task.switchToWriteMode();

  FileWriter::addStreamSourceToWriterModule(stream_settings, fw_task);

  // Skip the streamcontroller, topics and partitions bits and jump straight to the writing for now
  send_f144_data_to_source(fw_task, "delay_source_chopper", 123, 123456);
  send_f144_data_to_source(fw_task, "delay_source_chopper", 124, 123457);

  return 0;
}
