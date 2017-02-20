#include "SchemaRegistry.h"
#include "HDFFile.h"
#include "HDFFile_h5.h"
#include <hdf5.h>
#include "schemas/f141_epics_nt_generated.h"

namespace BrightnESS {
namespace FileWriter {
namespace Schemas {
namespace f141 {

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

class reader : public FBSchemaReader {
std::unique_ptr<FBSchemaWriter> create_writer_impl() override;
std::string sourcename_impl(Msg msg) override;
uint64_t ts_impl(Msg msg) override;
uint64_t teamid_impl(Msg & msg) override;
};

class writer : public FBSchemaWriter {
~writer() override;
void init_impl(std::string const & sourcename, Msg msg) override;
WriteResult write_impl(Msg msg) override;
using DT = double;
// DataSet::getId() will be -1 for the default constructed
hid_t ds = -1;
hid_t dsp = -1;
hid_t dcpl = -1;
hid_t ds_seq_data = -1;
hid_t ds_ts_data = -1;
bool do_flush_always = true;
};

std::unique_ptr<FBSchemaWriter> reader::create_writer_impl() {
	return std::unique_ptr<FBSchemaWriter>(new writer);
}

static FlatBufs::f141_epics_nt::fwdinfo_2_t const * fwdinfo(FlatBufs::f141_epics_nt::EpicsPV const * buf) {
	if (buf->fwdinfo2_type() != FlatBufs::f141_epics_nt::fwdinfo_u::fwdinfo_2_t) {
		return 0;
	}
	auto fi = (FlatBufs::f141_epics_nt::fwdinfo_2_t const *) buf->fwdinfo2();
	return fi;
}

std::string reader::sourcename_impl(Msg msg) {
	auto epicspv = FlatBufs::f141_epics_nt::GetEpicsPV(msg.data);
	if (!epicspv->name()) {
		LOG(3, "WARNING message has no source name");
		return "";
	}
	return epicspv->name()->str();
}

// TODO
// Should be in64_t to handle error cases
uint64_t reader::ts_impl(Msg msg) {
	auto epicspv = FlatBufs::f141_epics_nt::GetEpicsPV(msg.data);
	auto fi = fwdinfo(epicspv);
	if (!fi) {
		LOG(3, "ERROR no time data sent");
		return 0;
	}
	return fi->ts_data();
}


uint64_t reader::teamid_impl(Msg & msg) {
	auto epicspv = FlatBufs::f141_epics_nt::GetEpicsPV(msg.data);
	auto fi = fwdinfo(epicspv);
	if (!fi) {
		LOG(3, "ERROR no fwdinfo data sent");
		return 0;
	}
	return fi->teamid();
}


writer::~writer() {
	if (ds != -1) H5Dclose(ds);
	if (ds_seq_data != -1) H5Dclose(ds_seq_data);
	if (ds_ts_data != -1) H5Dclose(ds_ts_data);
	if (dsp != -1) H5Sclose(dsp);
	if (dcpl != -1) H5Pclose(dcpl);
}


void writer::init_impl(std::string const & sourcename, Msg msg) {
	// TODO
	// This is just a unbuffered, low-performance write.
	// Add buffering after it works.
	auto file = hdf_file->h5file_detail().h5file();
	auto epicspv = FlatBufs::f141_epics_nt::GetEpicsPV(msg.data);
	// TODO verify buffer
	if (epicspv->pv_type() != FlatBufs::f141_epics_nt::PV::NTScalarArrayDouble) {
		LOG(2, "SORRY cant handle that at the moment");
		return;
	}
	auto pv = (FlatBufs::f141_epics_nt::NTScalarArrayDouble*) epicspv->pv();
	if (!pv) {
		LOG(2, "ERROR no data was sent");
		return;
	}
	auto value = pv->value();
	if (!value) {
		LOG(2, "ERROR no value");
		return;
	}
	auto ncols = value->size();
	LOG(1, "f141_epics_nt::init_impl  v.size() == {}", ncols);
	using std::vector;
	using std::array;
	auto dt = nat_type<DT>();

	// H5S_UNLIMITED
	std::array<hsize_t, 2> sizes_ini {{0, ncols}};
	std::array<hsize_t, 2> sizes_max {{H5S_UNLIMITED, ncols}};

	// TODO
	// Lifetime?  Must it live as long as dataset exists?
	this->dsp = H5Screate_simple(sizes_ini.size(), sizes_ini.data(), sizes_max.data());
	if (true) {
		// Just check if it works as I think it should
		LOG(0, "DataSpace isSimple {}", H5Sis_simple(dsp));
		auto ndims = H5Sget_simple_extent_ndims(dsp);
		LOG(0, "DataSpace getSimpleExtentNdims {}", ndims);
		LOG(0, "DataSpace getSimpleExtentNpoints {}", H5Sget_simple_extent_npoints(dsp));
		std::vector<hsize_t> get_sizes_now;
		std::vector<hsize_t> get_sizes_max;
		get_sizes_now.resize(ndims);
		get_sizes_max.resize(ndims);
		H5Sget_simple_extent_dims(dsp, get_sizes_now.data(), get_sizes_max.data());
		for (int i1 = 0; i1 < ndims; ++i1) {
			LOG(0, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
		}
	}

	// TODO
	// Lifetime of this guy?
	// Together with the dataset?
	this->dcpl = H5Pcreate(H5P_DATASET_CREATE);
	std::array<hsize_t, 2> sizes_chk {{std::max(64*1024/H5Tget_size(dt)/ncols, (size_t)1), ncols}};
	H5Pset_chunk(dcpl, sizes_chk.size(), sizes_chk.data());
	this->ds = H5Dcreate1(file, sourcename.c_str(), dt, dsp, dcpl);

	{
		// Dataset for sequence numbers, used primarily for unit tests
		//template <int N> using A = array<hsize_t, N>;
		auto dt = nat_type<uint64_t>();
		auto ncols = 1;
		array<hsize_t, 2> sini {{ 0, 1 }};
		array<hsize_t, 2> smax {{ H5S_UNLIMITED, 1 }};
		auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
		auto dcpl = H5Pcreate(H5P_DATASET_CREATE);
		array<hsize_t, 2> schk {{std::max(64*1024/H5Tget_size(dt)/ncols, (size_t)1), 1}};
		herr_t err = H5Pset_chunk(dcpl, schk.size(), schk.data());
		if (err < 0) {
			LOG(7, "ERROR in H5Pset_chunk");
		}
		this->ds_seq_data = H5Dcreate1(file, (sourcename + "__seq_data").c_str(), dt, dsp, dcpl);
		if (this->ds_seq_data < 0) {
			LOG(7, "ERROR creating ds_seq_data");
		}
		H5Pclose(dcpl);
		H5Sclose(dsp);
	}

	{
		// Dataset for ts_data
		//template <int N> using A = array<hsize_t, N>;
		auto dt = nat_type<uint64_t>();
		auto ncols = 1;
		array<hsize_t, 2> sini {{ 0, 1 }};
		array<hsize_t, 2> smax {{ H5S_UNLIMITED, 1 }};
		auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
		auto dcpl = H5Pcreate(H5P_DATASET_CREATE);
		array<hsize_t, 2> schk {{ std::max(64*1024/H5Tget_size(dt)/ncols, (size_t)1), 1 }};
		herr_t err = H5Pset_chunk(dcpl, schk.size(), schk.data());
		if (err < 0) {
			LOG(7, "ERROR in H5Pset_chunk");
		}
		this->ds_ts_data = H5Dcreate1(file, (sourcename + "__ts_data").c_str(), dt, dsp, dcpl);
		if (this->ds_ts_data < 0) {
			LOG(7, "ERROR creating ds_ts_data");
		}
		H5Pclose(dcpl);
		H5Sclose(dsp);
	}
}



template <typename Td>
static int append_data(hid_t ds, Td const * data, size_t nlen) {
	auto tgt = H5Dget_space(ds);
	auto ndims = H5Sget_simple_extent_ndims(tgt);
	std::vector<hsize_t> get_sizes_now;
	std::vector<hsize_t> get_sizes_max;
	get_sizes_now.resize(ndims);
	get_sizes_max.resize(ndims);
	herr_t err;
	H5Sget_simple_extent_dims(tgt, get_sizes_now.data(), get_sizes_max.data());
	for (int i1 = 0; i1 < ndims; ++i1) {
		LOG(-1, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
	}
	if (get_sizes_now.at(1) != nlen) {
		LOG(3, "ERROR number of columns does not match");
		return -1;
	}

	get_sizes_now.at(0) += 1;
	err = H5Dextend(ds, get_sizes_now.data());
	if (err < 0) {
		LOG(7, "ERROR can not extend dataset");
		return {-1};
	}
	H5Sclose(tgt);

	tgt = H5Dget_space(ds);
	using A = std::array<hsize_t, 2>;
	A mem_size = {{1, get_sizes_now.at(1)}};
	auto mem = H5Screate_simple(2, mem_size.data(), nullptr);
	{
		A hsl_start {{0, 0}};
		A hsl_count {{1, get_sizes_now.at(1)}};
		err = H5Sselect_hyperslab(mem, H5S_SELECT_SET, hsl_start.data(), nullptr, hsl_count.data(), nullptr);
		if (err < 0) {
			LOG(7, "ERROR can not select mem hyperslab");
			return -2;
		}
	}
	{
		A hsl_start {{get_sizes_now.at(0)-1, 0}};
		A hsl_count {{1, get_sizes_now.at(1)}};
		err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, hsl_start.data(), nullptr, hsl_count.data(), nullptr);
		if (err < 0) {
			LOG(7, "ERROR can not select tgt hyperslab");
			return -3;
		}
	}
	for (int i1 = 0; i1 < ndims; ++i1) {
		LOG(-1, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
	}
	auto dt = nat_type<Td>();
	err = H5Dwrite(ds, dt, mem, tgt, H5P_DEFAULT, data);
	if (err < 0) {
		LOG(7, "ERROR writing failed");
		return -4;
	}
	err = H5Sclose(mem);
	return 0;
}



WriteResult writer::write_impl(Msg msg) {
	LOG(0, "f141_epics_nt::write_impl");
	herr_t err;
	auto veri = flatbuffers::Verifier((uint8_t*)msg.data, msg.size);
	if (!BrightnESS::FlatBufs::f141_epics_nt::VerifyEpicsPVBuffer(veri)) {
		LOG(3, "ERROR bad flat buffer");
		return {-1};
	}
	auto epicspv = FlatBufs::f141_epics_nt::GetEpicsPV(msg.data);
	auto pv = (FlatBufs::f141_epics_nt::NTScalarArrayDouble*)epicspv->pv();

	append_data(this->ds, pv->value()->data(), pv->value()->size());

	int64_t ts_data = -1;

	auto fi = fwdinfo(epicspv);
	if (fi) {
		LOG(3, "ts_data: {}", fi->ts_data());
		ts_data = (int64_t) fi->ts_data();
		auto x1 = fi->seq_data();
		append_data(this->ds_seq_data, &x1, 1);
		append_data(this->ds_ts_data, &ts_data, 1);
	}

	if (do_flush_always) {
		auto file = hdf_file->h5file_detail().h5file();
		err = H5Fflush(file, H5F_SCOPE_LOCAL);
		if (err < 0) {
			LOG(3, "ERROR while flushing");
		}
	}
	return {ts_data};
}



class Info : public SchemaInfo {
public:
FBSchemaReader::ptr create_reader() override;
};

FBSchemaReader::ptr Info::create_reader() {
	return FBSchemaReader::ptr(new reader);
}


SchemaRegistry::Registrar<Info> g_registrar(fbid_from_str("f141"));


}
}
}
}
