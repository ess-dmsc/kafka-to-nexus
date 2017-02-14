#include "HDFFile.h"
#include "HDFFile_h5.h"
#include <array>
#include <hdf5.h>
#include "SchemaRegistry.h"
#include "logger.h"
#include <flatbuffers/flatbuffers.h>


namespace BrightnESS {
namespace FileWriter {

class HDFFile_impl {
	friend class HDFFile;
	hid_t h5file = -1;
};

HDFFile::HDFFile() {
	impl.reset(new HDFFile_impl);
}

HDFFile::~HDFFile() {
	if (impl->h5file >= 0) {
		H5Fclose(impl->h5file);
	}
}

void HDFFile::init(std::string filename) {
	impl->h5file = H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
}

void HDFFile::flush() {
	H5Fflush(impl->h5file, H5F_SCOPE_LOCAL);
}

HDFFile_h5 HDFFile::h5file_detail() {
	return HDFFile_h5(impl->h5file);
}

HDFFile_h5::HDFFile_h5(hid_t h5file) : _h5file(h5file) {
}

hid_t HDFFile_h5::h5file() {
	return _h5file;
}

std::unique_ptr<FBSchemaReader> FBSchemaReader::create(Msg msg) {
	static_assert(FLATBUFFERS_LITTLEENDIAN, "Requires currently little endian");
	if (msg.size < 8) {
		LOG(3, "ERROR message is too small");
		return nullptr;
	}
	Schemas::FBID fbid;
	memcpy(&fbid, msg.data + 4, 4);
	if (auto & cr = Schemas::SchemaRegistry::find(fbid)) {
		return cr->create_reader();
	}
	return nullptr;
}

std::unique_ptr<FBSchemaWriter> FBSchemaReader::create_writer() {
	return create_writer_impl();
}

FBSchemaReader::~FBSchemaReader() {
}

std::string FBSchemaReader::sourcename(Msg msg) {
	return sourcename_impl(msg);
}

uint64_t FBSchemaReader::ts(Msg msg) {
	return ts_impl(msg);
}

uint64_t FBSchemaReader::teamid(Msg & msg) {
	return teamid_impl(msg);
}

uint64_t FBSchemaReader::teamid_impl(Msg & msg) {
	return 0;
}


FBSchemaWriter::FBSchemaWriter() {
}

FBSchemaWriter::~FBSchemaWriter() {
}

void FBSchemaWriter::init(HDFFile * hdf_file, std::string const & sourcename, Msg msg) {
	this->hdf_file = hdf_file;
	init_impl(sourcename, msg);
}

WriteResult FBSchemaWriter::write(Msg msg) {
	return write_impl(msg);
}



}
}
