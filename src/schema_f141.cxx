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
template <> hid_t nat_type<uint32_t>() { return H5T_NATIVE_UINT32; }
template <> hid_t nat_type<uint64_t>() { return H5T_NATIVE_UINT64; }

class reader : public FBSchemaReader {
std::unique_ptr<FBSchemaWriter> create_writer_impl() override;
std::string sourcename_impl(char * msg_data) override;
uint64_t ts_impl(char * msg_data) override;
};

class writer : public FBSchemaWriter {
void init_impl(std::string const & sourcename, char * msg_data) override;
WriteResult write_impl(char * msg_data) override;
using DT = double;
// DataSet::getId() will be -1 for the default constructed
hid_t ds = -1;
hid_t dsp = -1;
hid_t dcpl = -1;
};

std::unique_ptr<FBSchemaWriter> reader::create_writer_impl() {
	return std::unique_ptr<FBSchemaWriter>(new writer);
}

std::string reader::sourcename_impl(char * msg_data) {
	auto epicspv = FlatBufs::f141_epics_nt::GetEpicsPV(msg_data);
	if (!epicspv->name()) {
		LOG(3, "WARNING message has no source name");
		return "";
	}
	return epicspv->name()->str();
}

// TODO
// Should be in64_t to handle error cases
uint64_t reader::ts_impl(char * msg_data) {
	auto epicspv = FlatBufs::f141_epics_nt::GetEpicsPV(msg_data);
	auto fwdinfo = epicspv->fwdinfo();
	if (!fwdinfo) {
		LOG(3, "ERROR no time data sent");
		return 0;
	}
	return fwdinfo->ts_data();
}



void writer::init_impl(std::string const & sourcename, char * msg_data) {
	// TODO
	// This is just a unbuffered, low-performance write.
	// Add buffering after it works.
	auto file = hdf_file->h5file_detail().h5file();
	auto epicspv = FlatBufs::f141_epics_nt::GetEpicsPV(msg_data);
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
	std::array<hsize_t, 2> sizes_max {{H5S_UNLIMITED, H5S_UNLIMITED}};

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
}


WriteResult writer::write_impl(char * msg_data) {
	LOG(0, "f141_epics_nt::write_impl");
	// TODO cache these values later, but for now, let's make it verbose:

	// Just being explicit here
	auto & ds = this->ds;
	auto tgt = H5Dget_space(ds);
	auto ndims = H5Sget_simple_extent_ndims(tgt);
	std::vector<hsize_t> get_sizes_now;
	std::vector<hsize_t> get_sizes_max;
	get_sizes_now.resize(ndims);
	get_sizes_max.resize(ndims);
	H5Sget_simple_extent_dims(tgt, get_sizes_now.data(), get_sizes_max.data());
	for (int i1 = 0; i1 < ndims; ++i1) {
		LOG(0, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
	}

	auto epicspv = FlatBufs::f141_epics_nt::GetEpicsPV(msg_data);
	auto pv = (FlatBufs::f141_epics_nt::NTScalarArrayDouble*)epicspv->pv();
	if (pv->value()->size() != get_sizes_now.at(1)) {
		LOG(7, "ERROR this message is not compatible with the previous ones");
		return {-1};
	}

	get_sizes_now.at(0) += 1;
	herr_t err;
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
			return {-1};
		}
	}
	{
		A hsl_start {{get_sizes_now.at(0)-1, 0}};
		A hsl_count {{1, get_sizes_now.at(1)}};
		err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, hsl_start.data(), nullptr, hsl_count.data(), nullptr);
		if (err < 0) {
			LOG(7, "ERROR can not select tgt hyperslab");
			return {-1};
		}
	}
	for (int i1 = 0; i1 < ndims; ++i1) {
		LOG(0, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
	}
	auto dt = nat_type<DT>();
	err = H5Dwrite(ds, dt, mem, tgt, H5P_DEFAULT, pv->value()->data());
	if (err < 0) {
		LOG(7, "ERROR writing failed");
		return {-1};
	}
	H5Sclose(mem);
	auto fwdinfo = epicspv->fwdinfo();
	if (!fwdinfo) {
		LOG(3, "ERROR no time data sent");
		return {-1};
	}
	return {(int64_t)fwdinfo->ts_data()};
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
