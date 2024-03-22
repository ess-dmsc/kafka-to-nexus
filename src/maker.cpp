#include <iostream>
#include <memory>
#include <fmt/format.h>
#include "logger.h"
#include "FileWriterTask.h"
#include "Metrics/Metric.h"
#include "MetaData/Tracker.h"

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
								"source": "delay_source_chopper",
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
}

class FakeRegistrar : public Metrics::IRegistrar {
public:
  void registerMetric(Metrics::Metric &NewMetric,
                      std::vector<Metrics::LogTo> const &SinkTypes) const override {
    UNUSED_ARG(NewMetric);
    UNUSED_ARG(SinkTypes);
  }
};

class FakeTracker: public MetaData::ITracker {
public:
  void registerMetaData(MetaData::ValueBase NewMetaData) override {
    UNUSED_ARG(NewMetaData);
  }
  void clearMetaData() override {}
  void writeToJSONDict(nlohmann::json &JSONNode) const override{
    UNUSED_ARG(JSONNode);
  }
  void writeToHDF5File(hdf5::node::Group RootNode) const override{
    UNUSED_ARG(RootNode);
  }
};

int main(int argc, char **argv) {
  UNUSED_ARG(argc);
  UNUSED_ARG(argv);
  std::cout << "hello from the maker app\n";

  FakeRegistrar registrar;
  auto tracker = std::make_shared<FakeTracker>();

  FileWriter::FileWriterTask fw_task{&registrar, tracker};
  fw_task.setFullFilePath("", "example.hdf");

  std::vector<ModuleHDFInfo> module_info_collector;
  fw_task.InitialiseHdf(example_json, module_info_collector);

  return 0;
}
