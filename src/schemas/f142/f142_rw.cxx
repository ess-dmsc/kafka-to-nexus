#include "../../SchemaRegistry.h"
#include "../../HDFFile.h"
#include "../../HDFFile_h5.h"
#include <hdf5.h>
#include "schemas/f142_logdata_generated.h"

namespace BrightnESS {
namespace FileWriter {
namespace Schemas {
namespace f142 {

template <typename T> using uptr = std::unique_ptr<T>;

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
virtual int write_impl(LogData const * fbuf) = 0;
};

template <typename DT>
class writer_typed_array : public writer_typed_base {
public:
template <typename FV>
writer_typed_array(hid_t hdf_group, std::string const & sourcename, FV fbval);
~writer_typed_array();
int write_impl(LogData const * fbuf);
// DataSet::getId() will be -1 for the default constructed
hid_t ds = -1;
hid_t dsp = -1;
hid_t dcpl = -1;
};

class writer : public FBSchemaWriter {
~writer() override;
void init_impl(std::string const & sourcename, hid_t hdf_group, Msg msg) override;
WriteResult write_impl(Msg msg) override;
uptr<writer_typed_base> impl;
hid_t ds_seq_data = -1;
hid_t ds_seq_fwd = -1;
hid_t ds_ts_data = -1;
bool do_flush_always = false;
bool do_writer_forwarder_internal = false;
};

std::unique_ptr<FBSchemaWriter> reader::create_writer_impl() {
	return std::unique_ptr<FBSchemaWriter>(new writer);
}

bool reader::verify_impl(Msg msg) {
	auto veri = flatbuffers::Verifier((uint8_t*)msg.data, msg.size);
	if (VerifyLogDataBuffer(veri)) return true;
	return false;
}

std::string reader::sourcename_impl(Msg msg) {
	auto fbuf = GetLogData(msg.data);
	if (!fbuf->source_name()) {
		LOG(4, "WARNING message has no source name");
		return "";
	}
	return fbuf->source_name()->str();
}

uint64_t reader::ts_impl(Msg msg) {
	auto fbuf = GetLogData(msg.data);
	return fbuf->timestamp();
}

uint64_t reader::teamid_impl(Msg & msg) {
	auto fbuf = GetLogData(msg.data);
	if (fbuf->fwdinfo_type() == forwarder_internal::fwdinfo_1_t) {
		return ((fwdinfo_1_t*)fbuf->fwdinfo())->teamid();
	}
	return 0;
}


writer::~writer() {
	if (ds_seq_data != -1) H5Dclose(ds_seq_data);
	if (ds_seq_fwd != -1) H5Dclose(ds_seq_fwd);
	if (ds_ts_data != -1) H5Dclose(ds_ts_data);
}

template <typename T> using WA = writer_typed_array<T>;

void writer::init_impl(std::string const & sourcename, hid_t hdf_group, Msg msg) {
	using std::array;
	// This is just a unbuffered, low-performance write.
	// Improved write on separate branch.
	// Want to gather performance data first for this baseline implementation
	// in the integration tests.
	auto fbuf = GetLogData(msg.data);
	auto & hg = hdf_group;
	auto & s = sourcename;

	// TODO
	// add remaining, and also the scalar values!

	auto impl_fac = [&hg, &s, &fbuf](Value x){
		using R = writer_typed_base *;
		void const * v = fbuf->value();
		if (x == Value::ArrayByte)   return (R) new WA<int8_t>  (hg, s, (ArrayByte*)v);
		if (x == Value::ArrayShort)  return (R) new WA<int16_t> (hg, s, (ArrayShort*)v);
		if (x == Value::ArrayInt)    return (R) new WA<int32_t> (hg, s, (ArrayInt*)v);
		if (x == Value::ArrayLong)   return (R) new WA<int64_t> (hg, s, (ArrayLong*)v);
		if (x == Value::ArrayUByte)  return (R) new WA<uint8_t> (hg, s, (ArrayUByte*)v);
		if (x == Value::ArrayUShort) return (R) new WA<uint16_t>(hg, s, (ArrayUShort*)v);
		if (x == Value::ArrayUInt)   return (R) new WA<uint32_t>(hg, s, (ArrayUInt*)v);
		if (x == Value::ArrayULong)  return (R) new WA<uint64_t>(hg, s, (ArrayULong*)v);
		if (x == Value::ArrayDouble) return (R) new WA<double>  (hg, s, (ArrayDouble*)v);
		if (x == Value::ArrayFloat)  return (R) new WA<float>   (hg, s, (ArrayFloat*)v);
		return (writer_typed_base*) nullptr;
	};
	impl.reset(impl_fac(fbuf->value_type()));

	if (do_writer_forwarder_internal) {
		// Dataset for sequence numbers, used primarily for unit tests
		auto dt = nat_type<uint64_t>();
		auto ncols = 1;
		array<hsize_t, 2> sini {{ 0, 1 }};
		array<hsize_t, 2> smax {{ H5S_UNLIMITED, 1 }};
		auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
		auto dcpl = H5Pcreate(H5P_DATASET_CREATE);
		array<hsize_t, 2> schk {{std::max<hsize_t>(64*1024/H5Tget_size(dt)/ncols, 1), 1}};
		herr_t err = H5Pset_chunk(dcpl, schk.size(), schk.data());
		if (err < 0) {
			LOG(0, "ERROR in H5Pset_chunk");
		}
		this->ds_seq_data = H5Dcreate1(hdf_group, (sourcename + "__fwdinfo_seq_data").c_str(), dt, dsp, dcpl);
		if (this->ds_seq_data < 0) {
			LOG(0, "ERROR creating ds_seq_data");
		}
		H5Pclose(dcpl);
		H5Sclose(dsp);
	}
	if (do_writer_forwarder_internal) {
		// Dataset seq_fwd
		auto dt = nat_type<uint64_t>();
		auto ncols = 1;
		array<hsize_t, 2> sini {{ 0, 1 }};
		array<hsize_t, 2> smax {{ H5S_UNLIMITED, 1 }};
		auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
		auto dcpl = H5Pcreate(H5P_DATASET_CREATE);
		array<hsize_t, 2> schk {{std::max<hsize_t>(64*1024/H5Tget_size(dt)/ncols, 1), 1}};
		herr_t err = H5Pset_chunk(dcpl, schk.size(), schk.data());
		if (err < 0) {
			LOG(0, "ERROR in H5Pset_chunk");
		}
		this->ds_seq_fwd = H5Dcreate1(hdf_group, (sourcename + "__fwdinfo_seq_fwd").c_str(), dt, dsp, dcpl);
		if (this->ds_seq_fwd < 0) {
			LOG(0, "ERROR creating ds_seq_fwd");
		}
		H5Pclose(dcpl);
		H5Sclose(dsp);
	}
	if (do_writer_forwarder_internal) {
		// Dataset for ts_data
		auto dt = nat_type<uint64_t>();
		auto ncols = 1;
		array<hsize_t, 2> sini {{ 0, 1 }};
		array<hsize_t, 2> smax {{ H5S_UNLIMITED, 1 }};
		auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
		auto dcpl = H5Pcreate(H5P_DATASET_CREATE);
		array<hsize_t, 2> schk {{ std::max<hsize_t>(64*1024/H5Tget_size(dt)/ncols, 1), 1 }};
		herr_t err = H5Pset_chunk(dcpl, schk.size(), schk.data());
		if (err < 0) {
			LOG(0, "ERROR in H5Pset_chunk");
		}
		this->ds_ts_data = H5Dcreate1(hdf_group, (sourcename + "__fwdinfo_ts_data").c_str(), dt, dsp, dcpl);
		if (this->ds_ts_data < 0) {
			LOG(0, "ERROR creating ds_ts_data");
		}
		H5Pclose(dcpl);
		H5Sclose(dsp);
	}
}



template <typename DT>
writer_typed_array<DT>::~writer_typed_array() {
	if (ds != -1) H5Dclose(ds);
	if (dsp != -1) H5Sclose(dsp);
	if (dcpl != -1) H5Pclose(dcpl);
}


template <typename DT>
template <typename FV>
writer_typed_array<DT>::writer_typed_array(hid_t hdf_group, std::string const & sourcename, FV fv) {
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
	this->ds = H5Dcreate1(hdf_group, sourcename.c_str(), dt, dsp, dcpl);
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


template <typename DT>
int writer_typed_array<DT>::write_impl(LogData const * fbuf) {
	auto value1 = (ArrayDouble*)fbuf->value();
	append_data(this->ds, value1->value()->data(), value1->value()->size());
	return 0;
}


WriteResult writer::write_impl(Msg msg) {
	auto fbuf = GetLogData(msg.data);
	if (impl->write_impl(fbuf)) {
		LOG(5, "write failed");
	}
	if (do_writer_forwarder_internal) {
		if (fbuf->fwdinfo_type() == forwarder_internal::fwdinfo_1_t) {
			auto fi = (fwdinfo_1_t*)fbuf->fwdinfo();
			auto ts_data = (int64_t) fi->ts_data();
			auto seq_data = fi->seq_data();
			auto seq_fwd = fi->seq_fwd();
			append_data(this->ds_seq_data, &seq_data, 1);
			append_data(this->ds_seq_fwd, &seq_fwd, 1);
			append_data(this->ds_ts_data, &ts_data, 1);
		}
	}

	if (do_flush_always) {
		auto file = hdf_file->h5file_detail().h5file();
		auto err = H5Fflush(file, H5F_SCOPE_LOCAL);
		if (err < 0) {
			LOG(4, "ERROR while flushing");
		}
	}
	return {(int64_t)fbuf->timestamp()};
}



class Info : public SchemaInfo {
public:
FBSchemaReader::ptr create_reader() override;
};

FBSchemaReader::ptr Info::create_reader() {
	return FBSchemaReader::ptr(new reader);
}


SchemaRegistry::Registrar<Info> g_registrar(fbid_from_str("f142"));


}
}
}
}
