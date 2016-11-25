#include "NexusWriter.h"
#include <rapidjson/document.h>
#include "logger.h"

namespace BrightnESS {
namespace FileWriter {

NexusWriter::NexusWriter(rapidjson::Document & conf) {
	// Open hdf file

	// Parse configuration to add meta data

	// With schema validation, json document should be safe to use
	auto streams = conf.FindMember("streams")->value.GetArray();
	for (auto & s : streams) {
		LOG(3, "have a stream: {}", s["topic"].GetString());
	}
}

/// Dummy so far.
/// We want to publish later info like:
/// Health status of this writer, written data volume so far, data bandwidth, ...
int NexusWriter::status() {
	return 0;
}

}
}
