#include "../../SchemaRegistry.h"
#include "../../HDFFile.h"
#include "../../HDFFile_h5.h"
#include "../../helper.h"
#include <hdf5.h>
#include <limits>
#include "schemas/ev42_events_generated.h"

namespace BrightnESS {
namespace FileWriter {
namespace Schemas {
namespace ev42 {

using std::array;
using std::vector;
using std::string;


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
hid_t ds_event_id = -1;
hid_t ds_event_time_offset = -1;
hid_t ds_event_time_zero = -1;
hid_t ds_event_index = -1;
hid_t ds_cue_timestamp_zero = -1;
hid_t ds_cue_index = -1;
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
	if (ds_event_id != -1) H5Dclose(ds_event_id);
	if (ds_event_time_offset != -1) H5Dclose(ds_event_time_offset);
	if (ds_event_time_zero != -1) H5Dclose(ds_event_time_zero);
	if (ds_event_index != -1) H5Dclose(ds_event_index);
	if (ds_cue_timestamp_zero != -1) H5Dclose(ds_cue_timestamp_zero);
	if (ds_cue_index != -1) H5Dclose(ds_cue_index);
}


template <typename T> hid_t nat_type();
template <> hid_t nat_type<float>()    { return H5T_NATIVE_FLOAT; }
template <> hid_t nat_type<double>()   { return H5T_NATIVE_DOUBLE; }
template <> hid_t nat_type<int8_t>() { return H5T_NATIVE_INT8; }
template <> hid_t nat_type<int16_t>() { return H5T_NATIVE_INT16; }
template <> hid_t nat_type<int32_t>() { return H5T_NATIVE_INT32; }
template <> hid_t nat_type<int64_t>() { return H5T_NATIVE_INT64; }
template <> hid_t nat_type<uint8_t>() { return H5T_NATIVE_UINT8; }
template <> hid_t nat_type<uint16_t>() { return H5T_NATIVE_UINT16; }
template <> hid_t nat_type<uint32_t>() { return H5T_NATIVE_UINT32; }
template <> hid_t nat_type<uint64_t>() { return H5T_NATIVE_UINT64; }


template <typename T>
static hid_t create_dataset(hid_t loc, string name, uint32_t chunk_bytes) {
	// event data is just a continuous blob of numbers...
	using A1 = array<hsize_t, 1>;
	hid_t ret = -1;
	auto dt = nat_type<T>();
	// Sizes, at initialization and maximum
	A1 sini {{ 0 }};
	A1 smax {{ H5S_UNLIMITED }};
	auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
	auto dcpl = H5Pcreate(H5P_DATASET_CREATE);
	A1 schk {{ std::max<hsize_t>(chunk_bytes/H5Tget_size(dt), 1) }};
	H5Pset_chunk(dcpl, schk.size(), schk.data());
	ret = H5Dcreate1(loc, name.c_str(), dt, dsp, dcpl);
	H5Pclose(dcpl);
	H5Sclose(dsp);
	return ret;
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

	this->ds_event_time_offset = create_dataset<uint32_t>(hdf_group, "event_time_offset", 1*1024*1024);
	this->ds_event_id = create_dataset<uint32_t>(hdf_group, "event_id", 1*1024*1024);
	this->ds_event_time_zero = create_dataset<uint64_t>(hdf_group, "event_time_zero", 128*1024);
	this->ds_event_index = create_dataset<uint32_t>(hdf_group, "event_index", 64*1024);
	this->ds_cue_timestamp_zero = create_dataset<uint64_t>(hdf_group, "cue_timestamp_zero", 128*1024);
	this->ds_cue_index = create_dataset<uint32_t>(hdf_group, "cue_index", 64*1024);
}


template <typename Td>
static append_ret append_data_1d(hid_t ds, Td const * data, size_t nlen) {
	if (log_level >= 9) {
		array<char, 64> buf1;
		auto n1 = H5Iget_name(ds, buf1.data(), buf1.size());
		if (n1 > 0) {
			LOG(9, "append_data_1d {} for dataset {:.{}}", nlen, buf1.data(), n1);
		}
	}
	auto dt = nat_type<Td>();
	auto tgt = H5Dget_space(ds);
	//auto ndims = H5Sget_simple_extent_ndims(tgt);
	using A1 = array<hsize_t, 1>;
	A1 snow;
	A1 smax;
	herr_t err;
	H5Sget_simple_extent_dims(tgt, snow.data(), smax.data());
	if (log_level >= 9) {
		for (size_t i1 = 0; i1 < snow.size(); ++i1) {
			LOG(9, "H5Sget_simple_extent_dims {:3}", snow.at(i1));
		}
	}

	snow[0] += nlen;
	err = H5Dextend(ds, snow.data());
	if (err < 0) {
		LOG(3, "ERROR can not extend dataset");
		H5Sclose(tgt);
		return {-1};
	}

	H5Sclose(tgt);

	tgt = H5Dget_space(ds);
	A1 mem = {{ nlen }};

	auto dsp_mem = H5Screate_simple(mem.size(), mem.data(), nullptr);
	{
		A1 start {{ 0 }};
		A1 count {{ nlen }};
		err = H5Sselect_hyperslab(dsp_mem, H5S_SELECT_SET, start.data(), nullptr, count.data(), nullptr);
		if (err < 0) {
			LOG(3, "ERROR can not select tgt hyperslab");
			return {-3};
		}
	}

	A1 tgt_start {{ snow.at(0)-nlen }};
	A1 tgt_count {{ nlen }};
	err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, tgt_start.data(), nullptr, tgt_count.data(), nullptr);
	if (err < 0) {
		LOG(3, "ERROR can not select tgt hyperslab");
		return {-3};
	}

	err = H5Dwrite(ds, dt, dsp_mem, tgt, H5P_DEFAULT, data);
	if (err < 0) {
		LOG(3, "ERROR writing failed");
		return {-4};
	}
	return {0, sizeof(Td) * nlen, tgt_start[0]};
}



WriteResult writer::write_impl(Msg msg) {
	// No buffering yet, just plain and simple writes for integration tests
	auto fbuf = get_fbuf(msg.data);
	int64_t ts = fbuf->pulse_time();
	auto w1ret = append_data_1d(this->ds_event_time_offset, fbuf->time_of_flight()->data(), fbuf->time_of_flight()->size());
	auto w2ret = append_data_1d(this->ds_event_id, fbuf->detector_id()->data(), fbuf->detector_id()->size());
	if (w1ret.ix0 != w2ret.ix0) {
		LOG(3, "written data lengths differ");
	}
	auto pulse_time = fbuf->pulse_time();
	append_data_1d(this->ds_event_time_zero, &pulse_time, 1);
	uint32_t event_index = w1ret.ix0;
	append_data_1d(this->ds_event_index, &event_index, 1);
	total_written_bytes += w1ret.written_bytes;
	total_written_bytes += w2ret.written_bytes;
	ts_max = std::max(pulse_time, ts_max);
	if (total_written_bytes > index_at_bytes + index_every_bytes) {
		append_data_1d(this->ds_cue_timestamp_zero, &ts_max, 1);
		append_data_1d(this->ds_cue_index, &event_index, 1);
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
