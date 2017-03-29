#include "../../SchemaRegistry.h"
#include "../../HDFFile.h"
#include "../../HDFFile_h5.h"
#include "../../helper.h"
#include <hdf5.h>
#include "h5.h"
#include <limits>
#include "schemas/ev42_events_generated.h"

namespace BrightnESS {
namespace FileWriter {
namespace Schemas {
namespace ev42 {

using std::array;
using std::vector;
using std::string;
template <typename T> using uptr = std::unique_ptr<T>;


class reader : public FBSchemaReader {
std::unique_ptr<FBSchemaWriter> create_writer_impl() override;
bool verify_impl(Msg msg) override;
std::string sourcename_impl(Msg msg) override;
uint64_t ts_impl(Msg msg) override;
};


struct append_ret {
int status;
uint64_t written_bytes;
uint64_t ix0;
operator bool () const { return status == 0; }
};

class writer : public FBSchemaWriter {
~writer() override;
void init_impl(std::string const & sourcename, hid_t hdf_group, Msg msg) override;
WriteResult write_impl(Msg msg) override;
uptr<h5::h5d_chunked_1d> ds_event_id;
uptr<h5::h5d_chunked_1d> ds_event_time_offset;
uptr<h5::h5d_chunked_1d> ds_event_time_zero;
uptr<h5::h5d_chunked_1d> ds_event_index;
uptr<h5::h5d_chunked_1d> ds_cue_index;
uptr<h5::h5d_chunked_1d> ds_cue_timestamp_zero;
bool do_flush_always = false;
uint64_t total_written_bytes = 0;
uint64_t index_at_bytes = 0;
uint64_t index_every_bytes = std::numeric_limits<uint64_t>::max();
uint64_t ts_max = 0;
};


std::unique_ptr<FBSchemaWriter> reader::create_writer_impl() {
	return std::unique_ptr<FBSchemaWriter>(new writer);
}

static EventMessage const * get_fbuf(char const * data) {
	return GetEventMessage(data);
}

bool reader::verify_impl(Msg msg) {
	auto veri = flatbuffers::Verifier((uint8_t*)msg.data, msg.size);
	if (VerifyEventMessageBuffer(veri)) return true;
	return false;
}

uint64_t reader::ts_impl(Msg msg) {
	auto fbuf = get_fbuf(msg.data);
	return fbuf->pulse_time();
}


std::string reader::sourcename_impl(Msg msg) {
	auto fbuf = get_fbuf(msg.data);
	auto v = fbuf->source_name();
	if (!v) {
		LOG(4, "WARNING message has no source name");
		return "";
	}
	return v->str();
}


writer::~writer() {
}


void writer::init_impl(std::string const & sourcename, hid_t hdf_group, Msg msg) {
	LOG(7, "ev42::init_impl");

	if (config_file) {
		if (auto x = get_int(config_file, "nexus.indices.index_every_kb")) {
			index_every_bytes = (int)x * 1024;
		}
		else if (auto x = get_int(config_file, "nexus.indices.index_every_mb")) {
			index_every_bytes = (int)x * 1024*1024;
		}
	}

	this->ds_event_time_offset.reset(new h5::h5d_chunked_1d(hdf_group, "event_time_offset", 1*1024*1024, uint32_t(0)));
	this->ds_event_id.reset(new h5::h5d_chunked_1d(hdf_group, "event_id", 1*1024*1024, uint32_t(0)));
	this->ds_event_time_zero.reset(new h5::h5d_chunked_1d(hdf_group, "event_time_zero", 128*1024, uint64_t(0)));
	this->ds_event_index.reset(new h5::h5d_chunked_1d(hdf_group, "event_index", 64*1024, uint32_t(0)));
	this->ds_cue_index.reset(new h5::h5d_chunked_1d(hdf_group, "cue_index", 64*1024, uint32_t(0)));
	this->ds_cue_timestamp_zero.reset(new h5::h5d_chunked_1d(hdf_group, "cue_timestamp_zero", 128*1024, uint64_t(0)));
}


WriteResult writer::write_impl(Msg msg) {
	// No buffering yet, just plain and simple writes for integration tests
	auto fbuf = get_fbuf(msg.data);
	int64_t ts = fbuf->pulse_time();
	auto w1ret = this->ds_event_time_offset->append_data_1d(fbuf->time_of_flight()->data(), fbuf->time_of_flight()->size());
	auto w2ret = this->ds_event_id->append_data_1d(fbuf->detector_id()->data(), fbuf->detector_id()->size());
	if (w1ret.ix0 != w2ret.ix0) {
		LOG(3, "written data lengths differ");
	}
	auto pulse_time = fbuf->pulse_time();
	this->ds_event_time_zero->append_data_1d(&pulse_time, 1);
	uint32_t event_index = w1ret.ix0;
	this->ds_event_index->append_data_1d(&event_index, 1);
	total_written_bytes += w1ret.written_bytes;
	ts_max = std::max(pulse_time, ts_max);
	if (total_written_bytes > index_at_bytes + index_every_bytes) {
		this->ds_cue_timestamp_zero->append_data_1d(&ts_max, 1);
		this->ds_cue_index->append_data_1d(&event_index, 1);
		index_at_bytes = total_written_bytes;
	}
	if (do_flush_always) {
		auto file = hdf_file->h5file_detail().h5file();
		auto err = H5Fflush(file, H5F_SCOPE_LOCAL);
		if (err < 0) {
			LOG(4, "ERROR while flushing");
		}
	}
	return {ts};
}


class Info : public SchemaInfo {
public:
FBSchemaReader::ptr create_reader() override;
};

FBSchemaReader::ptr Info::create_reader() {
	return FBSchemaReader::ptr(new reader);
}

SchemaRegistry::Registrar<Info> g_registrar(fbid_from_str("ev42"));

}
}
}
}
