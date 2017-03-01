#include "HDFFile.h"
#include "HDFFile_impl.h"
#include "HDFFile_h5.h"
#include <array>
#include <deque>
#include <chrono>
#include <ctime>
#include <hdf5.h>
#include "SchemaRegistry.h"
#include "logger.h"
#include <flatbuffers/flatbuffers.h>
#include <unistd.h>
#include "date/date.h"
#define HAS_REMOTE_API 0
#include "date/tz.h"


namespace BrightnESS {
namespace FileWriter {

using std::string;
using std::vector;
using std::array;

HDFFile::HDFFile() {
	// Keep this.  Will be used later to test against different lib versions
	#if H5_VERSION_GE(1, 8, 0) && H5_VERSION_LE(1, 10, 99)
		unsigned int maj, min, rel;
		H5get_libversion(&maj, &min, &rel);
	#else
		static_assert(false, "Unexpected HDF version");
	#endif

	impl.reset(new HDFFile_impl);
}

HDFFile::~HDFFile() {
	if (impl->h5file >= 0) {
		std::array<char, 512> fname;
		H5Fget_name(impl->h5file, fname.data(), fname.size());
		LOG(5, "flush file {}", fname.data());
		H5Fflush(impl->h5file, H5F_SCOPE_LOCAL);
		H5Fclose(impl->h5file);
	}
}


template <size_t const N> using AA = std::array<hsize_t, N>;

static void write_hdf_ds_scalar_string(hid_t loc, std::string name, std::string s1) {
	auto strfix = H5Tcopy(H5T_C_S1);
	H5Tset_cset(strfix, H5T_CSET_UTF8);
	H5Tset_size(strfix, s1.size());
	using A = AA<1>;
	A sini {{ 1 }};
	A smax {{ 1 }};
	auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
	auto ds = H5Dcreate2(loc, name.c_str(), strfix, dsp, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	H5Dwrite(ds, strfix, H5S_ALL, H5S_ALL, H5P_DEFAULT, s1.data());
	H5Dclose(ds);
	H5Sclose(dsp);
	H5Tclose(strfix);
}


template <typename T>
static void write_hdf_iso8601(hid_t loc, std::string name, T & ts) {
	using namespace date;
	using namespace std::chrono;
	auto s2 = format("%Y-%m-%dT%H:%M:%S%z", ts);
	write_hdf_ds_scalar_string(loc, name, s2.c_str());
}


static void write_attribute(hid_t loc, std::string name, std::string value) {
	auto acpl = H5Pcreate(H5P_ATTRIBUTE_CREATE);
	H5Pset_char_encoding(acpl, H5T_CSET_UTF8);
	auto dsp_sc = H5Screate(H5S_SCALAR);
	auto strfix = H5Tcopy(H5T_C_S1);
	H5Tset_cset(strfix, H5T_CSET_UTF8);
	H5Tset_size(strfix, value.size());
	auto at = H5Acreate2(loc, name.c_str(), strfix, dsp_sc, acpl, H5P_DEFAULT);
	H5Awrite(at, strfix, value.data());
	H5Aclose(at);
	H5Tclose(strfix);
	H5Sclose(dsp_sc);
	H5Pclose(acpl);
}


int HDFFile::init(std::string filename, rapidjson::Value const & nexus_structure) {
	using std::string;
	using std::vector;
	using rapidjson::Value;
	auto x = H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
	if (x < 0) {
		std::array<char, 256> cwd;
		getcwd(cwd.data(), cwd.size());
		LOG(0, "ERROR could not create the HDF file: {}  cwd: {}", filename, cwd.data());
		return -1;
	}
	LOG(5, "INFO opened the file hid: {}", x);
	impl->h5file = x;

	auto lcpl = H5Pcreate(H5P_LINK_CREATE);
	H5Pset_char_encoding(lcpl, H5T_CSET_UTF8);
	auto acpl = H5Pcreate(H5P_ATTRIBUTE_CREATE);
	H5Pset_char_encoding(acpl, H5T_CSET_UTF8);
	auto strfix = H5Tcopy(H5T_C_S1);
	H5Tset_cset(strfix, H5T_CSET_UTF8);
	H5Tset_size(strfix, 1);
	auto dsp_sc = H5Screate(H5S_SCALAR);

	auto f1 = x;

	if (nexus_structure.IsObject()) {
		// Traverse nexus_structure
		// The rapidjson visitor interface is not flexible enough unfortunately
		struct SE {
			string name;
			Value const * jsv;
			hid_t nxparent;
			hid_t nxv;
			Value::ConstMemberIterator itr;
			bool basics;
			string nx_type {"group"};
			SE(string name, Value const * jsv, hid_t nxparent) :
					name(name),
					jsv(jsv),
					nxparent(nxparent),
					nxv(-1),
					itr(jsv->MemberEnd()),
					basics(false)
			{
				if (jsv->IsObject()) {
					itr = jsv->MemberBegin();
				}
			}
		};
		std::deque<SE> stack;
		stack.push_back(SE{"/", &nexus_structure, -1});
		stack.back().nxv = f1;
		while (stack.size() > 0) {
			auto & se = stack.back();
			LOG(3, "NOW AT level: {}  name: {}", stack.size(), se.name);
			for (auto & x : stack) {
				string itrname = "[empty]";
				if (x.itr != x.jsv->MemberEnd()) {
					itrname = x.itr->name.GetString();
				}
				LOG(3, "  itrname: {}", itrname);
			}
			auto & jsv = se.jsv;
			if (!se.basics) {
				auto mem = jsv->FindMember("NX_type");
				if (mem != jsv->MemberEnd()) {
					auto & nx_type = mem->value;
					if (nx_type.IsString()) {
						char const * s = nx_type.GetString();
						LOG(3, "nx_type: {} {}", (void*)&s, s[0]);
						if (string("field") == s) {
							se.nx_type = "field";
						}
						else if (string("some-other") == s) {
						}
					}
				}

				if (se.nxv == -1) {
					if (se.nx_type == "group") {
						se.nxv = H5Gcreate2(se.nxparent, se.name.c_str(), lcpl, H5P_DEFAULT, H5P_DEFAULT);
					}
				}

				mem = jsv->FindMember("NX_class");
				if (mem != jsv->MemberEnd()) {
					auto & nx_class = mem->value;
					if (nx_class.IsString()) {
						write_attribute(se.nxv, "NX_class", nx_class.GetString());
					}
				}

				mem = jsv->FindMember("NX_attributes");
				if (mem != jsv->MemberEnd()) {
					auto & nx_class = mem->value;
					if (nx_class.IsString()) {
						write_attribute(se.nxv, "NX_class", nx_class.GetString());
					}
				}

				// TODO
				// Check if there are NX_attributes and set them

				se.basics = true;
			}

			bool have_child = false;
			if (se.jsv->IsObject()) {
				while (!have_child && se.itr != se.jsv->MemberEnd()) {
					LOG(3, "consider name: {}", se.itr->name.GetString());
					if (strncmp("NX_", se.itr->name.GetString(), 3) != 0) {
						if (se.itr->value.IsObject()) {
							stack.emplace_back(se.itr->name.GetString(), &se.itr->value, se.nxv);
							LOG(3, "pushed name: {}", stack.back().name);
							have_child = true;
						}
						else if (se.itr->value.IsString()) {
							// Treated as a basic scalar string "nexus field" aka "hdf dataset"
							string s1 = se.itr->value.GetString();
							auto dsp = H5Screate(H5S_SCALAR);
							H5Tset_size(strfix, s1.size());
							auto ds = H5Dcreate2(se.nxv, se.itr->name.GetString(), strfix, dsp, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
							H5Dwrite(ds, strfix, H5S_ALL, H5S_ALL, H5P_DEFAULT, s1.data());
							H5Dclose(ds);
							H5Sclose(dsp);
						}
						else if (se.itr->value.IsInt64()) {
							int64_t val = se.itr->value.GetInt64();
							auto dsp = H5Screate(H5S_SCALAR);
							auto ds = H5Dcreate2(se.nxv, se.itr->name.GetString(), H5T_NATIVE_INT64, dsp, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
							H5Dwrite(ds, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, &val);
							H5Dclose(ds);
							H5Sclose(dsp);
						}
						else if (se.itr->value.IsDouble()) {
							double val = se.itr->value.GetDouble();
							auto dsp = H5Screate(H5S_SCALAR);
							auto ds = H5Dcreate2(se.nxv, se.itr->name.GetString(), H5T_NATIVE_DOUBLE, dsp, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
							H5Dwrite(ds, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, &val);
							H5Dclose(ds);
							H5Sclose(dsp);
						}
					}
					++se.itr;
				}
			}

			if (!have_child) {
				stack.pop_back();
			}
		}
	}

	if (false) {

	{
		auto payload = filename;
		H5Tset_size(strfix, payload.size());
		auto at = H5Acreate2(f1, "file_name", strfix, dsp_sc, acpl, H5P_DEFAULT);
		H5Awrite(at, strfix, payload.data());
		H5Aclose(at);
		//H5Eprint2(H5E_DEFAULT, nullptr);
	}

	{
		vector<char> s1(64);
		if (false) {
			// std way
			using namespace std::chrono;
			auto now = system_clock::now();
			auto time = system_clock::to_time_t(now);
			strftime(s1.data(), s1.size(), "%Y-%m-%dT%H:%M:%S%z", localtime(&time));
		}
		{
			// date way
			using namespace date;
			using namespace std::chrono;
			auto now2 = make_zoned(current_zone(), floor<milliseconds>(system_clock::now()));
			auto s2 = format("%Y-%m-%dT%H:%M:%S%z", now2);
			std::copy(s2.c_str(), s2.c_str() + s2.size(), s1.data());
		}
		H5Tset_size(strfix, s1.size());
		auto at = H5Acreate2(f1, "file_time", strfix, dsp_sc, acpl, H5P_DEFAULT);
		H5Awrite(at, strfix, s1.data());
		H5Aclose(at);
	}

	{
		// top level NXentry
		auto g1 = H5Gcreate2(f1, "entry", lcpl, H5P_DEFAULT, H5P_DEFAULT);
		{
			string s1 {"NXentry"};
			H5Tset_size(strfix, s1.size());
			auto at = H5Acreate2(g1, "NX_class", strfix, dsp_sc, acpl, H5P_DEFAULT);
			H5Awrite(at, strfix, s1.data());
			H5Aclose(at);
		}
		{
			// set application definition
			string s1("NXreftof");
			using A = AA<1>;
			A sini {{ 1 }};
			A smax {{ 1 }};
			auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
			H5Tset_size(strfix, s1.size());
			auto ds = H5Dcreate2(g1, "definition", strfix, dsp, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
			H5Dwrite(ds, strfix, H5S_ALL, H5S_ALL, H5P_DEFAULT, s1.data());
			H5Dclose(ds);
		}
		{
			// set application definition
			string s1("Some title here");
			using A = AA<1>;
			A sini {{ 1 }};
			A smax {{ 1 }};
			auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
			H5Tset_size(strfix, s1.size());
			auto ds = H5Dcreate2(g1, "title", strfix, dsp, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
			H5Dwrite(ds, strfix, H5S_ALL, H5S_ALL, H5P_DEFAULT, s1.data());
			H5Dclose(ds);
		}
		{
			using namespace date;
			using namespace std::chrono;
			auto now = make_zoned(current_zone(), floor<milliseconds>(system_clock::now()));
			auto end = make_zoned(current_zone(), floor<milliseconds>(system_clock::now() + seconds(10)));
			write_hdf_iso8601(g1, "start_time", now);
			write_hdf_iso8601(g1, "end_time", end);
		}
		{
			// NXinstrument
			auto g_inst = H5Gcreate2(g1, "instrument", lcpl, H5P_DEFAULT, H5P_DEFAULT);
			{
				string s1 {"NXinstrument"};
				H5Tset_size(strfix, s1.size());
				auto at = H5Acreate2(g_inst, "NX_class", strfix, dsp_sc, acpl, H5P_DEFAULT);
				H5Awrite(at, strfix, s1.data());
				H5Aclose(at);
			}
			write_hdf_ds_scalar_string(g_inst, "name", "instrument-name");
			{
				auto g_chopper = H5Gcreate2(g_inst, "chopper", lcpl, H5P_DEFAULT, H5P_DEFAULT);
				{
					string s1 {"NXdisk_chopper"};
					H5Tset_size(strfix, s1.size());
					auto at = H5Acreate2(g_chopper, "NX_class", strfix, dsp_sc, acpl, H5P_DEFAULT);
					H5Awrite(at, strfix, s1.data());
					H5Aclose(at);
				}
				write_hdf_ds_scalar_string(g_chopper, "distance", "the-chopper-distance...");
			}
		}
	}

	}

	H5Sclose(dsp_sc);
	H5Pclose(lcpl);
	H5Pclose(acpl);

	return 0;
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
		LOG(4, "ERROR message is too small");
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
	if (group_event_data != -1) H5Gclose(group_event_data);
}

void FBSchemaWriter::init(HDFFile * hdf_file, string const & sourcename, Msg msg) {
	this->hdf_file = hdf_file;
	auto f1 = hdf_file->h5file_detail().h5file();

	auto group_name = std::string("data-") + sourcename;

	// Create the event data group
	auto lcpl = H5Pcreate(H5P_LINK_CREATE);
	H5Pset_char_encoding(lcpl, H5T_CSET_UTF8);
	auto acpl = H5Pcreate(H5P_ATTRIBUTE_CREATE);
	H5Pset_char_encoding(acpl, H5T_CSET_UTF8);
	auto strfix = H5Tcopy(H5T_C_S1);
	H5Tset_cset(strfix, H5T_CSET_UTF8);
	H5Tset_size(strfix, 1);
	auto dsp_sc = H5Screate(H5S_SCALAR);
	group_event_data = H5Gcreate2(f1, group_name.c_str(), lcpl, H5P_DEFAULT, H5P_DEFAULT);
	{
		string s1 {"NXevent_data"};
		H5Tset_size(strfix, s1.size());
		auto at = H5Acreate2(group_event_data, "NX_class", strfix, dsp_sc, acpl, H5P_DEFAULT);
		H5Awrite(at, strfix, s1.data());
		H5Aclose(at);
	}
	H5Pclose(lcpl);
	H5Pclose(acpl);
	H5Tclose(strfix);
	H5Sclose(dsp_sc);

	init_impl(sourcename, group_event_data, msg);
}

WriteResult FBSchemaWriter::write(Msg msg) {
	return write_impl(msg);
}



}
}
