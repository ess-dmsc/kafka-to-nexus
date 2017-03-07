#include "../../SchemaRegistry.h"
#include "../../HDFFile.h"
#include "../../HDFFile_h5.h"
#include "../../helper.h"
#include <hdf5.h>
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
std::string sourcename_impl(Msg msg) override;
uint64_t ts_impl(Msg msg) override;
};


class writer : public FBSchemaWriter {
~writer() override;
void init_impl(std::string const & sourcename, hid_t hdf_group, Msg msg) override;
WriteResult write_impl(Msg msg) override;
hid_t ds_time_of_flight = -1;
hid_t ds_detector_id = -1;
bool do_flush_always = true;
};


std::unique_ptr<FBSchemaWriter> reader::create_writer_impl() {
	return std::unique_ptr<FBSchemaWriter>(new writer);
}


uint64_t reader::ts_impl(Msg msg) {
	return 818181;
}


std::string reader::sourcename_impl(Msg msg) {
	auto epicspv = GetEventMessage(msg.data);
	auto v = epicspv->source_name();
	if (!v) {
		LOG(4, "WARNING message has no source name");
		return "";
	}
	return v->str();
}


writer::~writer() {
	if (ds_time_of_flight != -1) H5Dclose(ds_time_of_flight);
	if (ds_detector_id != -1) H5Dclose(ds_detector_id);
}


static hid_t create_dataset(hid_t loc, string name) {
	// event data is just a continuous blob of numbers...
	using A1 = array<hsize_t, 1>;
	hid_t ret = -1;
	auto dt = H5T_NATIVE_UINT32;
	// Sizes, at initialization and maximum
	A1 sini {{ 0 }};
	A1 smax {{ H5S_UNLIMITED }};
	auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
	auto dcpl = H5Pcreate(H5P_DATASET_CREATE);
	A1 schk {{ std::max(4*1024*1024/H5Tget_size(dt), (size_t)1) }};
	H5Pset_chunk(dcpl, schk.size(), schk.data());
	ret = H5Dcreate1(loc, name.c_str(), dt, dsp, dcpl);
	H5Pclose(dcpl);
	H5Sclose(dsp);
	return ret;
}


void writer::init_impl(std::string const & sourcename, hid_t hdf_group, Msg msg) {
	LOG(7, "ev42::init_impl");
	auto veri = flatbuffers::Verifier((uint8_t*)msg.data, msg.size);
	if (!VerifyEventMessageBuffer(veri)) {
		LOG(3, "bad flat buffer");
		return;
	}
	auto evmsg = GetEventMessage(msg.data);
	this->ds_detector_id = create_dataset(hdf_group, "detector_id");
	this->ds_time_of_flight = create_dataset(hdf_group, "time_of_flight");
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

template <typename Td>
static int append_data_1d(hid_t ds, Td const * data, size_t nlen) {
	auto dt = nat_type<Td>();
	auto tgt = H5Dget_space(ds);
	//auto ndims = H5Sget_simple_extent_ndims(tgt);
	using A1 = array<hsize_t, 1>;
	A1 snow;
	A1 smax;
	herr_t err;
	H5Sget_simple_extent_dims(tgt, snow.data(), smax.data());
	for (size_t i1 = 0; i1 < snow.size(); ++i1) {
		LOG(9, "H5Sget_simple_extent_dims {:3} {:3}", snow.at(i1), smax.at(i1));
	}

	snow[0] += nlen;
	err = H5Dextend(ds, snow.data());
	if (err < 0) {
		LOG(0, "ERROR can not extend dataset");
		H5Sclose(tgt);
		return -1;
	}

	H5Sclose(tgt);

	tgt = H5Dget_space(ds);
	A1 mem = {{ nlen }};

	// TODO make this a class member?
	static hid_t dsp_mem = -1;
	if (dsp_mem == -1) {
		dsp_mem = H5Screate_simple(mem.size(), mem.data(), nullptr);
	}
	{
		A1 start {{ 0 }};
		A1 count {{ nlen }};
		err = H5Sselect_hyperslab(dsp_mem, H5S_SELECT_SET, start.data(), nullptr, count.data(), nullptr);
		if (err < 0) {
			LOG(0, "ERROR can not select tgt hyperslab");
			return -3;
		}
	}
	{
		A1 start {{ snow.at(0)-nlen }};
		A1 count {{ nlen }};
		err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, start.data(), nullptr, count.data(), nullptr);
		if (err < 0) {
			LOG(0, "ERROR can not select tgt hyperslab");
			return -3;
		}
	}
	for (size_t i1 = 0; i1 < snow.size(); ++i1) {
		LOG(9, "H5Sget_simple_extent_dims {:3} {:3}", snow.at(i1), smax.at(i1));
	}
	err = H5Dwrite(ds, dt, dsp_mem, tgt, H5P_DEFAULT, data);
	if (err < 0) {
		LOG(0, "ERROR writing failed");
		return -4;
	}
	return -1;
}



WriteResult writer::write_impl(Msg msg) {
	LOG(7, "ev42::write_impl");
	// No buffering yet, just plain and simple writes for integration tests
	auto veri = flatbuffers::Verifier((uint8_t*)msg.data, msg.size);
	if (!VerifyEventMessageBuffer(veri)) {
		LOG(3, "bad flat buffer");
		return {-1};
	}
	auto evmsg = GetEventMessage(msg.data);
	int64_t ts = evmsg->pulse_time();
	LOG(9, "APPEND TO: {}, {}", ds_time_of_flight, evmsg->time_of_flight()->size());
	append_data_1d(this->ds_time_of_flight, evmsg->time_of_flight()->data(), evmsg->time_of_flight()->size());
	append_data_1d(this->ds_detector_id, evmsg->detector_id()->data(), evmsg->detector_id()->size());
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
