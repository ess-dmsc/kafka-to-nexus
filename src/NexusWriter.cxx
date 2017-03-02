#include "NexusWriter.h"
#include <rapidjson/document.h>
#include "logger.h"
#include <hdf5.h>
#include <H5Cpp.h>

#if HAVE_GTEST
#include <gtest/gtest.h>
#endif


//				static_assert(FLATBUFFERS_LITTLEENDIAN, "Requires currently little endian");


namespace BrightnESS {
namespace FileWriter {

NexusWriter::NexusWriter(rapidjson::Document & conf) {
	// Open hdf file

	// Parse configuration to add meta data

	// With schema validation, json document should be safe to use
	auto streams = conf.FindMember("streams")->value.GetArray();
	for (auto & s : streams) {
		LOG(4, "have a stream: {}", s["topic"].GetString());
	}
}

/// Dummy so far.
/// We want to publish later info like:
/// Health status of this writer, written data volume so far, data bandwidth, ...
int NexusWriter::status() {
	return 0;
}


#if HAVE_GTEST

TEST(hdf, proto_01) {
	LOG(4, "HDF playground (remove later)");
	// Size of a comound must be given and is specified in bytes.
	// When building up data types, HDF checks for overlap.
	size_t nbytes = 128;
	hid_t t1 = H5Tcreate(H5T_COMPOUND, nbytes);
	H5Tinsert(t1, "member1", 0, H5T_NATIVE_UINT64);
	H5Tinsert(t1, "member2", 8, H5T_NATIVE_DOUBLE);
}

#endif

}
}
