#include "../../SchemaRegistry.h"
#include "../../HDFFile.h"
#include "../../HDFFile_h5.h"
#include <hdf5.h>
#include "schemas/f141_epics_nt_generated.h"

namespace BrightnESS {
namespace FileWriter {
namespace Schemas {
namespace f141 {

using std::array;
using std::vector;
using std::string;
template <typename T> using uptr = std::unique_ptr<T>;
using FBUF = FlatBufs::f141_epics_nt::EpicsPV;

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
bool verify_impl(Msg msg) override;
std::string sourcename_impl(Msg msg) override;
uint64_t ts_impl(Msg msg) override;
uint64_t teamid_impl(Msg & msg) override;
};

class writer_typed_base {
public:
virtual int write_impl(FBUF const * fbuf) = 0;
};

template <typename DT, typename FV>
class writer_typed_array : public writer_typed_base {
public:
writer_typed_array(hid_t hdf_group, std::string const & sourcename, FV * fbval);
~writer_typed_array();
int write_impl(FBUF const * fbuf) override;
// DataSet::getId() will be -1 for the default constructed
hid_t ds = -1;
hid_t dsp = -1;
hid_t dcpl = -1;
};

template <typename DT, typename FV>
class writer_typed_scalar : public writer_typed_base {
public:
writer_typed_scalar(hid_t hdf_group, std::string const & sourcename, FV * fbval);
~writer_typed_scalar();
int write_impl(FBUF const * fbuf) override;
hid_t ds = -1;
hid_t dsp = -1;
hid_t dcpl = -1;
};

class writer : public FBSchemaWriter {
~writer() override;
void init_impl(std::string const & sourcename, hid_t hdf_group, Msg msg) override;
WriteResult write_impl(Msg msg) override;
uptr<writer_typed_base> impl;
hid_t ds_timestamp = -1;
hid_t ds_seq_data = -1;
hid_t ds_seq_fwd = -1;
hid_t ds_ts_data = -1;
bool do_flush_always = false;
bool do_writer_forwarder_internal = false;
};

static FBUF const * get_fbuf(char * data) {
	return FlatBufs::f141_epics_nt::GetEpicsPV(data);
}

static FlatBufs::f141_epics_nt::fwdinfo_2_t const * fwdinfo(FlatBufs::f141_epics_nt::EpicsPV const * buf) {
	if (buf->fwdinfo2_type() != FlatBufs::f141_epics_nt::fwdinfo_u::fwdinfo_2_t) {
		return 0;
	}
	auto fi = (FlatBufs::f141_epics_nt::fwdinfo_2_t const *) buf->fwdinfo2();
	return fi;
}

std::unique_ptr<FBSchemaWriter> reader::create_writer_impl() {
	return std::unique_ptr<FBSchemaWriter>(new writer);
}

bool reader::verify_impl(Msg msg) {
	auto veri = flatbuffers::Verifier((uint8_t*)msg.data, msg.size);
	if (FlatBufs::f141_epics_nt::VerifyEpicsPVBuffer(veri)) return true;
	return false;
}

std::string reader::sourcename_impl(Msg msg) {
	auto fbuf = get_fbuf(msg.data);
	auto s1 = fbuf->name();
	if (!s1) {
		LOG(4, "WARNING message has no source name");
		return "";
	}
	return s1->str();
}

uint64_t reader::ts_impl(Msg msg) {
	auto fbuf = get_fbuf(msg.data);
	auto fi = fwdinfo(fbuf);
	if (!fi) {
		LOG(4, "ERROR no time data sent");
		return 0;
	}
	return fi->ts_data();
}

uint64_t reader::teamid_impl(Msg & msg) {
	auto fbuf = get_fbuf(msg.data);
	auto fi = fwdinfo(fbuf);
	if (!fi) {
		LOG(4, "ERROR no fwdinfo data sent");
		return 0;
	}
	return fi->teamid();
}


writer::~writer() {
	if (ds_timestamp != -1) H5Dclose(ds_timestamp);
	if (ds_seq_data != -1) H5Dclose(ds_seq_data);
	if (ds_seq_fwd != -1) H5Dclose(ds_seq_fwd);
	if (ds_ts_data != -1) H5Dclose(ds_ts_data);
}

template <typename T, typename V> using WA = writer_typed_array<T,V>;
template <typename T, typename V> using WS = writer_typed_scalar<T,V>;

template <typename T>
static hid_t create_1d_ds(hid_t loc, std::string const & name) {
	hid_t ret = -1;
	// Dataset for sequence numbers, used primarily for unit tests
	using AA = std::array<hsize_t, 1>;
	auto dt = nat_type<T>();
	AA sini {{ 0 }};
	AA smax {{ H5S_UNLIMITED }};
	auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
	auto dcpl = H5Pcreate(H5P_DATASET_CREATE);
	AA schk {{std::max<hsize_t>(64*1024/H5Tget_size(dt), 1)}};
	herr_t err = H5Pset_chunk(dcpl, schk.size(), schk.data());
	if (err < 0) {
		LOG(5, "in H5Pset_chunk");
	}
	ret = H5Dcreate1(loc, name.c_str(), dt, dsp, dcpl);
	if (ret < 0) {
		LOG(5, "can not create {}", name);
	}
	H5Pclose(dcpl);
	H5Sclose(dsp);
	return ret;
}


void writer::init_impl(string const & sourcename, hid_t hdf_group, Msg msg) {
	// This is just a unbuffered, low-performance write.
	// Improved write on separate branch.
	// Want to gather performance data first for this baseline implementation
	// in the integration tests.
	auto fbuf = get_fbuf(msg.data);
	auto & hg = hdf_group;
	string s("value");

	using Value = FlatBufs::f141_epics_nt::PV;
	auto impl_fac = [&hg, &s, &fbuf](Value x){
		using R = writer_typed_base *;
		using namespace FlatBufs::f141_epics_nt;
		void const * v = fbuf->pv();
		if (x == Value::NTScalarByte)   return (R) new WS<int8_t,   NTScalarByte>  (hg, s, (NTScalarByte*)v);
		if (x == Value::NTScalarShort)  return (R) new WS<int16_t,  NTScalarShort> (hg, s, (NTScalarShort*)v);
		if (x == Value::NTScalarInt)    return (R) new WS<int32_t,  NTScalarInt>   (hg, s, (NTScalarInt*)v);
		if (x == Value::NTScalarLong)   return (R) new WS<int64_t,  NTScalarLong>  (hg, s, (NTScalarLong*)v);
		if (x == Value::NTScalarUByte)  return (R) new WS<uint8_t,  NTScalarUByte> (hg, s, (NTScalarUByte*)v);
		if (x == Value::NTScalarUShort) return (R) new WS<uint16_t, NTScalarUShort>(hg, s, (NTScalarUShort*)v);
		if (x == Value::NTScalarUInt)   return (R) new WS<uint32_t, NTScalarUInt>  (hg, s, (NTScalarUInt*)v);
		if (x == Value::NTScalarULong)  return (R) new WS<uint64_t, NTScalarULong> (hg, s, (NTScalarULong*)v);
		if (x == Value::NTScalarDouble) return (R) new WS<double,   NTScalarDouble>(hg, s, (NTScalarDouble*)v);
		if (x == Value::NTScalarFloat)  return (R) new WS<float,    NTScalarFloat> (hg, s, (NTScalarFloat*)v);
		if (x == Value::NTScalarArrayByte)    return (R) new WA<int8_t,  NTScalarArrayByte>  (hg, s, (NTScalarArrayByte*)v);
		if (x == Value::NTScalarArrayShort)  return (R) new WA<int16_t,  NTScalarArrayShort> (hg, s, (NTScalarArrayShort*)v);
		if (x == Value::NTScalarArrayInt)    return (R) new WA<int32_t,  NTScalarArrayInt>   (hg, s, (NTScalarArrayInt*)v);
		if (x == Value::NTScalarArrayLong)   return (R) new WA<int64_t,  NTScalarArrayLong>  (hg, s, (NTScalarArrayLong*)v);
		if (x == Value::NTScalarArrayUByte)  return (R) new WA<uint8_t,  NTScalarArrayUByte> (hg, s, (NTScalarArrayUByte*)v);
		if (x == Value::NTScalarArrayUShort) return (R) new WA<uint16_t, NTScalarArrayUShort>(hg, s, (NTScalarArrayUShort*)v);
		if (x == Value::NTScalarArrayUInt)   return (R) new WA<uint32_t, NTScalarArrayUInt>  (hg, s, (NTScalarArrayUInt*)v);
		if (x == Value::NTScalarArrayULong)  return (R) new WA<uint64_t, NTScalarArrayULong> (hg, s, (NTScalarArrayULong*)v);
		if (x == Value::NTScalarArrayDouble) return (R) new WA<double,   NTScalarArrayDouble>(hg, s, (NTScalarArrayDouble*)v);
		if (x == Value::NTScalarArrayFloat)  return (R) new WA<float,    NTScalarArrayFloat> (hg, s, (NTScalarArrayFloat*)v);
		return (writer_typed_base*) nullptr;
	};
	impl.reset(impl_fac(fbuf->pv_type()));

	this->ds_timestamp = create_1d_ds<uint64_t>(hdf_group, "time");

	if (do_writer_forwarder_internal) {
		this->ds_seq_data = create_1d_ds<uint64_t>(hdf_group, sourcename + "__fwdinfo_seq_data");
		this->ds_seq_fwd = create_1d_ds<uint64_t>(hdf_group, sourcename + "__fwdinfo_seq_fwd");
		this->ds_ts_data = create_1d_ds<uint64_t>(hdf_group, sourcename + "__fwdinfo_ts_data");
	}
}



template <typename DT, typename FV>
writer_typed_array<DT, FV>::~writer_typed_array() {
	if (ds != -1) H5Dclose(ds);
	if (dsp != -1) H5Sclose(dsp);
	if (dcpl != -1) H5Pclose(dcpl);
}


template <typename DT, typename FV>
writer_typed_array<DT, FV>::writer_typed_array(hid_t hdf_group, std::string const & dataset_name, FV * fv) {
	hsize_t ncols = fv->value()->size();
	LOG(7, "f142 init_impl  v.size(): {}", ncols);
	using std::vector;
	using std::array;
	auto dt = nat_type<DT>();

	std::array<hsize_t, 2> sizes_ini {{0, ncols}};
	std::array<hsize_t, 2> sizes_max {{H5S_UNLIMITED, ncols}};

	this->dsp = H5Screate_simple(sizes_ini.size(), sizes_ini.data(), sizes_max.data());
	if (true) {
		// Just check if it works as I think it should
		LOG(7, "DataSpace isSimple {}", H5Sis_simple(dsp));
		auto ndims = H5Sget_simple_extent_ndims(dsp);
		LOG(7, "DataSpace getSimpleExtentNdims {}", ndims);
		LOG(7, "DataSpace getSimpleExtentNpoints {}", H5Sget_simple_extent_npoints(dsp));
		std::vector<hsize_t> get_sizes_now;
		std::vector<hsize_t> get_sizes_max;
		get_sizes_now.resize(ndims);
		get_sizes_max.resize(ndims);
		H5Sget_simple_extent_dims(dsp, get_sizes_now.data(), get_sizes_max.data());
		for (int i1 = 0; i1 < ndims; ++i1) {
			LOG(7, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
		}
	}

	this->dcpl = H5Pcreate(H5P_DATASET_CREATE);
	std::array<hsize_t, 2> sizes_chk {{std::max(64*1024/H5Tget_size(dt)/ncols, (hsize_t)1), ncols}};
	H5Pset_chunk(dcpl, sizes_chk.size(), sizes_chk.data());
	this->ds = H5Dcreate1(hdf_group, dataset_name.c_str(), dt, dsp, dcpl);
}



template <typename Td>
static int append_data_array(hid_t ds, Td const * data, size_t nlen) {
	// Yes, verbose and more checks than necessary, but this is to gather
	// baseline performance data before merging the tuned branch.
	auto tgt = H5Dget_space(ds);
	auto ndims = H5Sget_simple_extent_ndims(tgt);
	std::vector<hsize_t> get_sizes_now;
	std::vector<hsize_t> get_sizes_max;
	get_sizes_now.resize(ndims);
	get_sizes_max.resize(ndims);
	herr_t err;
	H5Sget_simple_extent_dims(tgt, get_sizes_now.data(), get_sizes_max.data());
	if (false) {
		for (int i1 = 0; i1 < ndims; ++i1) {
			LOG(9, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
		}
	}
	if (get_sizes_now.at(1) != nlen) {
		LOG(4, "ERROR number of columns does not match");
		return -1;
	}

	get_sizes_now.at(0) += 1;
	err = H5Dextend(ds, get_sizes_now.data());
	if (err < 0) {
		LOG(4, "ERROR can not extend dataset");
		return -1;
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
			LOG(4, "ERROR can not select mem hyperslab");
			return -2;
		}
	}
	{
		A hsl_start {{get_sizes_now.at(0)-1, 0}};
		A hsl_count {{1, get_sizes_now.at(1)}};
		err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, hsl_start.data(), nullptr, hsl_count.data(), nullptr);
		if (err < 0) {
			LOG(4, "ERROR can not select tgt hyperslab");
			return -3;
		}
	}
	if (false) {
		for (int i1 = 0; i1 < ndims; ++i1) {
			LOG(9, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
		}
	}
	auto dt = nat_type<Td>();
	err = H5Dwrite(ds, dt, mem, tgt, H5P_DEFAULT, data);
	if (err < 0) {
		LOG(4, "ERROR writing failed");
		return -4;
	}
	err = H5Sclose(mem);
	return 0;
}


template <typename Td>
static int append_data_scalar(hid_t ds, Td const data) {
	// Yes, verbose and more checks than necessary, but this is to gather
	// baseline performance data before merging the tuned branch.
	auto tgt = H5Dget_space(ds);
	auto ndims = H5Sget_simple_extent_ndims(tgt);
	if (ndims != 1) {
		LOG(6, "this data space is expected to have one dimension");
		return -1;
	}
	using AA = std::array<hsize_t, 1>;
	AA get_sizes_now;
	AA get_sizes_max;
	herr_t err;
	H5Sget_simple_extent_dims(tgt, get_sizes_now.data(), get_sizes_max.data());
	if (false) {
		for (int i1 = 0; i1 < ndims; ++i1) {
			LOG(9, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
		}
	}

	get_sizes_now.at(0) += 1;
	err = H5Dextend(ds, get_sizes_now.data());
	if (err < 0) {
		LOG(4, "ERROR can not extend dataset");
		return -1;
	}
	H5Sclose(tgt);

	tgt = H5Dget_space(ds);
	AA mem_size = {{1}};
	auto mem = H5Screate_simple(1, mem_size.data(), nullptr);
	{
		AA hsl_start {{0}};
		AA hsl_count {{1}};
		err = H5Sselect_hyperslab(mem, H5S_SELECT_SET, hsl_start.data(), nullptr, hsl_count.data(), nullptr);
		if (err < 0) {
			LOG(4, "ERROR can not select mem hyperslab");
			return -2;
		}
	}
	{
		AA hsl_start {{get_sizes_now.at(0)-1}};
		AA hsl_count {{1}};
		err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, hsl_start.data(), nullptr, hsl_count.data(), nullptr);
		if (err < 0) {
			LOG(4, "ERROR can not select tgt hyperslab");
			return -3;
		}
	}
	if (false) {
		for (int i1 = 0; i1 < ndims; ++i1) {
			LOG(9, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
		}
	}
	auto dt = nat_type<Td>();
	err = H5Dwrite(ds, dt, mem, tgt, H5P_DEFAULT, &data);
	if (err < 0) {
		LOG(4, "ERROR writing failed");
		return -4;
	}
	err = H5Sclose(mem);
	return 0;
}


template <typename DT, typename FV>
int writer_typed_array<DT, FV>::write_impl(FBUF const * fbuf) {
	auto value1 = (FV const *)fbuf->pv();
	append_data_array(this->ds, value1->value()->data(), value1->value()->size());
	return 0;
}


template <typename DT, typename FV>
writer_typed_scalar<DT, FV>::~writer_typed_scalar() {
	if (ds != -1) H5Dclose(ds);
	if (dsp != -1) H5Sclose(dsp);
	if (dcpl != -1) H5Pclose(dcpl);
}


template <typename DT, typename FV>
writer_typed_scalar<DT, FV>::writer_typed_scalar(hid_t hdf_group, std::string const & dataset_name, FV * fv) {
	LOG(7, "f142 init_impl  scalar");
	using std::vector;
	using std::array;
	using AA = std::array<hsize_t, 1>;
	auto dt = nat_type<DT>();

	AA sizes_ini {{0}};
	AA sizes_max {{H5S_UNLIMITED}};

	this->dsp = H5Screate_simple(sizes_ini.size(), sizes_ini.data(), sizes_max.data());
	if (true) {
		// Just check if it works as I think it should
		LOG(7, "DataSpace isSimple {}", H5Sis_simple(dsp));
		auto ndims = H5Sget_simple_extent_ndims(dsp);
		LOG(7, "DataSpace getSimpleExtentNdims {}", ndims);
		LOG(7, "DataSpace getSimpleExtentNpoints {}", H5Sget_simple_extent_npoints(dsp));
		std::vector<hsize_t> get_sizes_now;
		std::vector<hsize_t> get_sizes_max;
		get_sizes_now.resize(ndims);
		get_sizes_max.resize(ndims);
		H5Sget_simple_extent_dims(dsp, get_sizes_now.data(), get_sizes_max.data());
		for (int i1 = 0; i1 < ndims; ++i1) {
			LOG(7, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
		}
	}

	this->dcpl = H5Pcreate(H5P_DATASET_CREATE);
	AA sizes_chk {{std::max<hsize_t>(64*1024/H5Tget_size(dt), 1)}};
	H5Pset_chunk(dcpl, sizes_chk.size(), sizes_chk.data());
	this->ds = H5Dcreate1(hdf_group, dataset_name.c_str(), dt, dsp, dcpl);
}


template <typename DT, typename FV>
int writer_typed_scalar<DT, FV>::write_impl(FBUF const * fbuf) {
	auto value1 = (FV const *)fbuf->pv();
	append_data_scalar(this->ds, value1->value());
	return 0;
}


WriteResult writer::write_impl(Msg msg) {
	auto fbuf = get_fbuf(msg.data);
	if (!impl) {
		LOG(5, "sorry, but we were unable to initialize for this kind of messages");
		return {-1};
	}
	if (impl->write_impl(fbuf)) {
		LOG(5, "write failed");
	}
	uint64_t ts = 0;
	if (auto fts = fbuf->timeStamp()) {
		ts = fts->secondsPastEpoch() * 1000000000;
		ts += fts->nanoseconds();
	}
	append_data_scalar(this->ds_timestamp, ts);
	if (do_writer_forwarder_internal) {
		if (fbuf->fwdinfo2_type() == FlatBufs::f141_epics_nt::fwdinfo_u::fwdinfo_2_t) {
			auto fi = (FlatBufs::f141_epics_nt::fwdinfo_2_t*)fbuf->fwdinfo2();
			append_data_scalar(this->ds_seq_data, fi->seq_data());
			append_data_scalar(this->ds_seq_fwd, fi->seq_fwd());
			append_data_scalar(this->ds_ts_data, fi->ts_data());
		}
	}

	if (do_flush_always) {
		auto file = hdf_file->h5file_detail().h5file();
		auto err = H5Fflush(file, H5F_SCOPE_LOCAL);
		if (err < 0) {
			LOG(4, "ERROR while flushing");
		}
	}
	return {(int64_t)ts};
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
