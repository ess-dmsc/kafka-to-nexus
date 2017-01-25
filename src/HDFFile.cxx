#include "HDFFile.h"
#include "HDFFile_h5.h"
#include <array>
#include <H5Cpp.h>
#include "f140-general_generated.h"
#include "f141-ntarraydouble_generated.h"
#include "logger.h"


namespace BrightnESS {
namespace FileWriter {

template <typename T> H5::PredType const & nat_type();
template <> H5::PredType const & nat_type<float>() { return H5::PredType::NATIVE_FLOAT; }
template <> H5::PredType const & nat_type<double>() { return H5::PredType::NATIVE_DOUBLE; }
template <> H5::PredType const & nat_type<uint32_t>() { return H5::PredType::NATIVE_UINT32; }



class HDFFile_impl {
friend class HDFFile;
H5::H5File h5file;
};

HDFFile::HDFFile() {
	impl.reset(new HDFFile_impl);
}

HDFFile::~HDFFile() {
}

void HDFFile::init(std::string filename) {
	impl->h5file = H5::H5File(filename, H5F_ACC_TRUNC);
}

void HDFFile::flush() {
	impl->h5file.flush(H5F_SCOPE_LOCAL);
}

HDFFile_h5 HDFFile::h5file_detail() {
	return HDFFile_h5(&impl->h5file);
}

HDFFile_h5::HDFFile_h5(H5::H5File * h5file) : _h5file(h5file) {
}

H5::H5File & HDFFile_h5::h5file() {
	return *_h5file;
}


namespace schemareaders {

class f140_general : public FBSchemaReader {
public:
f140_general();
std::unique_ptr<FBSchemaWriter> create_writer_impl();
std::string sourcename_impl(char * msg_data);
uint64_t ts_impl(char * msg_data);
};

class f141_ntarraydouble : public FBSchemaReader {
public:
f141_ntarraydouble();
std::unique_ptr<FBSchemaWriter> create_writer_impl();
std::string sourcename_impl(char * msg_data);
uint64_t ts_impl(char * msg_data);
};

}

namespace schemawriters {

class f140_general : public FBSchemaWriter {
public:
f140_general();
void init_impl(HDFFile & hdf_file, std::string const & sourcename, char * msg);
WriteResult write_impl(char * msg_data);
private:
H5::DataSet ds;
};

class f141_ntarraydouble : public FBSchemaWriter {
public:
f141_ntarraydouble();
void init_impl(HDFFile & hdf_file, std::string const & sourcename, char * msg);
WriteResult write_impl(char * msg_data);
private:
using DT = double;
// DataSet::getId() will be -1 for the default constructed
H5::DataSet ds;
};

}

namespace schemareaders {

f140_general::f140_general() {
}
std::unique_ptr<FBSchemaWriter> f140_general::create_writer_impl() {
	return std::unique_ptr<FBSchemaWriter>(new schemawriters::f140_general);
}
std::string f140_general::sourcename_impl(char * msg_data) {
	auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f140_general::GetPV(msg_data + 2);
	return pv->src()->str();
}
uint64_t f140_general::ts_impl(char * msg_data) {
	auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f140_general::GetPV(msg_data + 2);
	return pv->ts();
}

f141_ntarraydouble::f141_ntarraydouble() {
}
std::unique_ptr<FBSchemaWriter> f141_ntarraydouble::create_writer_impl() {
	return std::unique_ptr<FBSchemaWriter>(new schemawriters::f141_ntarraydouble);
}
std::string f141_ntarraydouble::sourcename_impl(char * msg_data) {
	auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f141_ntarraydouble::GetPV(msg_data + 2);
	return pv->src()->str();
}
uint64_t f141_ntarraydouble::ts_impl(char * msg_data) {
	auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f141_ntarraydouble::GetPV(msg_data + 2);
	return pv->ts();
}

}

namespace schemawriters {

f140_general::f140_general() {
}
void f140_general::init_impl(HDFFile & hdf_file, std::string const & sourcename, char * msg_data) {
	LOG(3, "f140_general init");
	auto & file = hdf_file.h5file_detail().h5file();
	auto fid = file.getId();
	LOG(3, "id of the file: {}  size: {}", fid, file.getFileSize());
	file.flush(H5F_SCOPE_LOCAL);
}
WriteResult f140_general::write_impl(char * msg_data) {
	return {-1};
}

f141_ntarraydouble::f141_ntarraydouble() {
}
void f141_ntarraydouble::init_impl(HDFFile & hdf_file, std::string const & sourcename, char * msg_data) {
	// TODO
	// This is just a unbuffered, low-performance write.
	// Add buffering after it works.
	auto & file = hdf_file.h5file_detail().h5file();
	auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f141_ntarraydouble::GetPV(msg_data + 2);
	LOG(1, "f141_ntarraydouble::init_impl  v.size() == {}", pv->v()->size());
	using std::vector;
	using std::array;
	auto dt = nat_type<DT>();
	// H5S_UNLIMITED
	std::array<hsize_t, 2> sizes_ini {0, 5};
	std::array<hsize_t, 2> sizes_max {H5S_UNLIMITED, H5S_UNLIMITED};
	//std::array<hsize_t, 2> sizes_max {20, 20};
	H5::DataSpace dsp(sizes_ini.size(), sizes_ini.data(), sizes_max.data());
	if (false) {
		// Just check if it works as I think it should
		LOG(0, "DataSpace isSimple {}", dsp.isSimple());
		LOG(0, "DataSpace getSimpleExtentNdims {}", dsp.getSimpleExtentNdims());
		LOG(0, "DataSpace getSimpleExtentNpoints {}", dsp.getSimpleExtentNpoints());
		auto ndims = dsp.getSimpleExtentNdims();
		std::vector<hsize_t> get_sizes_now;
		std::vector<hsize_t> get_sizes_max;
		get_sizes_now.resize(ndims);
		get_sizes_max.resize(ndims);
		dsp.getSimpleExtentDims(get_sizes_now.data(), get_sizes_max.data());
		for (int i1 = 0; i1 < ndims; ++i1) {
			LOG(0, "DataSpace getSimpleExtentDims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
		}
	}
	auto cprops = H5::DSetCreatPropList();
	std::array<hsize_t, 2> sizes_chk {10, 5};
	cprops.setChunk(sizes_chk.size(), sizes_chk.data());
	ds = file.createDataSet(sourcename, dt, dsp, cprops);
}

WriteResult f141_ntarraydouble::write_impl(char * msg_data) {
	LOG(0, "f141_ntarraydouble::write_impl");
	// TODO cache these values later, but for now, let's make it verbose:
	auto dt = nat_type<DT>();
	auto dsp = ds.getSpace();
	auto ndims = dsp.getSimpleExtentNdims();
	std::vector<hsize_t> get_sizes_now;
	std::vector<hsize_t> get_sizes_max;
	get_sizes_now.resize(ndims);
	get_sizes_max.resize(ndims);
	dsp.getSimpleExtentDims(get_sizes_now.data(), get_sizes_max.data());

	auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f141_ntarraydouble::GetPV(msg_data + 2);
	if (pv->v()->size() != get_sizes_now.at(1)) {
		LOG(7, "ERROR this message is not compatible with the previous ones");
		return {-1};
	}

	get_sizes_now.at(0) += 1;
	ds.extend(get_sizes_now.data());
	using A = std::array<hsize_t, 2>;
	auto tgt = ds.getSpace();
	hsize_t mem_size = 5;
	auto mem = H5::DataSpace(1, &mem_size, &mem_size);
	{
		A hsl_count {1, 5};
		A hsl_start {get_sizes_now.at(0)-1, 0};
		tgt.selectHyperslab(H5S_SELECT_SET, hsl_count.data(), hsl_start.data());
	}
	ds.write(pv->v()->data(), dt, mem, tgt);
	return {pv->ts()};
}

}


std::unique_ptr<FBSchemaReader> FBSchemaReader::create(char * msg_data, int msg_size) {
	static_assert(FLATBUFFERS_LITTLEENDIAN, "Requires currently little endian");
	auto sid = *((uint16_t*)msg_data);
	LOG(0, "sid: {:x}", sid);
	if (sid == 0xf140) {
		return std::unique_ptr<FBSchemaReader>(new schemareaders::f140_general);
	}
	if (sid == 0xf141) {
		return std::unique_ptr<FBSchemaReader>(new schemareaders::f141_ntarraydouble);
	}
	return nullptr;
}

std::unique_ptr<FBSchemaWriter> FBSchemaReader::create_writer() {
	return create_writer_impl();
}

std::string FBSchemaReader::sourcename(char * msg_data) {
	return sourcename_impl(msg_data);
}

uint64_t FBSchemaReader::ts(char * msg_data) {
	return ts_impl(msg_data);
}


FBSchemaWriter::FBSchemaWriter() {
}

FBSchemaWriter::~FBSchemaWriter() {
}

void FBSchemaWriter::init(HDFFile & hdf_file, std::string const & sourcename, char * msg_data) {
	init_impl(hdf_file, sourcename, msg_data);
}

WriteResult FBSchemaWriter::write(char * msg_data) {
	return write_impl(msg_data);
}



}
}
