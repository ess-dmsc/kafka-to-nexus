#include "HDFFile.h"
#include "HDFFile_h5.h"
#include "HDFFile_impl.h"
#include "SchemaRegistry.h"
#include "date/date.h"
#include "helper.h"
#include "logger.h"
#include <array>
#include <chrono>
#include <ctime>
#include <deque>
#include <flatbuffers/flatbuffers.h>
#include <hdf5.h>
#include <unistd.h>
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

static void write_hdf_ds_scalar_string(hid_t loc, std::string name,
                                       std::string s1) {
  auto strfix = H5Tcopy(H5T_C_S1);
  H5Tset_cset(strfix, H5T_CSET_UTF8);
  H5Tset_size(strfix, s1.size());
  using A = AA<1>;
  A sini{ { 1 } };
  A smax{ { 1 } };
  auto dsp = H5Screate_simple(sini.size(), sini.data(), smax.data());
  auto ds = H5Dcreate2(loc, name.c_str(), strfix, dsp, H5P_DEFAULT, H5P_DEFAULT,
                       H5P_DEFAULT);
  H5Dwrite(ds, strfix, H5S_ALL, H5S_ALL, H5P_DEFAULT, s1.data());
  H5Dclose(ds);
  H5Sclose(dsp);
  H5Tclose(strfix);
}

template <typename T>
static void write_hdf_iso8601(hid_t loc, std::string name, T &ts) {
  using namespace date;
  using namespace std::chrono;
  auto s2 = format("%Y-%m-%dT%H:%M:%S%z", ts);
  write_hdf_ds_scalar_string(loc, name, s2.c_str());
}

static void write_attribute_str(hid_t loc, std::string name,
                                char const *value) {
  auto acpl = H5Pcreate(H5P_ATTRIBUTE_CREATE);
  H5Pset_char_encoding(acpl, H5T_CSET_UTF8);
  auto dsp_sc = H5Screate(H5S_SCALAR);
  auto strfix = H5Tcopy(H5T_C_S1);
  H5Tset_cset(strfix, H5T_CSET_UTF8);
  H5Tset_size(strfix, strlen(value));
  auto at = H5Acreate2(loc, name.c_str(), strfix, dsp_sc, acpl, H5P_DEFAULT);
  H5Awrite(at, strfix, value);
  H5Aclose(at);
  H5Tclose(strfix);
  H5Sclose(dsp_sc);
  H5Pclose(acpl);
}

template <typename T> hid_t nat_type();
template <> hid_t nat_type<float>() { return H5T_NATIVE_FLOAT; }
template <> hid_t nat_type<double>() { return H5T_NATIVE_DOUBLE; }
template <> hid_t nat_type<int8_t>() { return H5T_NATIVE_INT8; }
template <> hid_t nat_type<int16_t>() { return H5T_NATIVE_INT16; }
template <> hid_t nat_type<int32_t>() { return H5T_NATIVE_INT32; }
template <> hid_t nat_type<int64_t>() { return H5T_NATIVE_INT64; }
template <> hid_t nat_type<uint8_t>() { return H5T_NATIVE_UINT8; }
template <> hid_t nat_type<uint16_t>() { return H5T_NATIVE_UINT16; }
template <> hid_t nat_type<uint32_t>() { return H5T_NATIVE_UINT32; }
template <> hid_t nat_type<uint64_t>() { return H5T_NATIVE_UINT64; }

template <typename T>
static void write_attribute(hid_t loc, std::string name, T value);

template <typename T>
static void write_attribute(hid_t loc, std::string name, T value) {
  auto acpl = H5Pcreate(H5P_ATTRIBUTE_CREATE);
  H5Pset_char_encoding(acpl, H5T_CSET_UTF8);
  auto dsp_sc = H5Screate(H5S_SCALAR);
  auto at =
      H5Acreate2(loc, name.c_str(), nat_type<T>(), dsp_sc, acpl, H5P_DEFAULT);
  H5Awrite(at, nat_type<T>(), &value);
  H5Aclose(at);
  H5Sclose(dsp_sc);
  H5Pclose(acpl);
}

int HDFFile::init(std::string filename,
                  rapidjson::Value const &nexus_structure) {
  using std::string;
  using std::vector;
  using rapidjson::Value;
  auto x = H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
  if (x < 0) {
    std::array<char, 256> cwd;
    getcwd(cwd.data(), cwd.size());
    LOG(0, "ERROR could not create the HDF file: {}  cwd: {}", filename,
        cwd.data());
    return -1;
  }
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
      Value const *jsv;
      hid_t nxparent;
      hid_t nxv;
      Value::ConstMemberIterator itr;
      bool basics;
      string nx_type{ "group" };
      SE(string name, Value const *jsv, hid_t nxparent)
          : name(name), jsv(jsv), nxparent(nxparent), nxv(-1),
            itr(jsv->MemberEnd()), basics(false) {
        if (jsv->IsObject()) {
          itr = jsv->MemberBegin();
        }
      }
    };
    std::deque<SE> stack;
    stack.push_back(SE{ "/", &nexus_structure, -1 });
    stack.back().nxv = f1;
    while (stack.size() > 0) {
      auto &se = stack.back();
      for (auto &x : stack) {
        string itrname = "[empty]";
        if (x.itr != x.jsv->MemberEnd()) {
          itrname = x.itr->name.GetString();
        }
      }
      auto &jsv = se.jsv;
      if (!se.basics) {
        auto mem = jsv->FindMember("NX_type");
        if (mem != jsv->MemberEnd()) {
          auto &nx_type = mem->value;
          if (nx_type.IsString()) {
            char const *s = nx_type.GetString();
            if (string("field") == s) {
              se.nx_type = "field";
            } else if (string("some-other") == s) {
            }
          }
        }

        if (se.nxv == -1) {
          if (se.nx_type == "group") {
            se.nxv = H5Gcreate2(se.nxparent, se.name.c_str(), lcpl, H5P_DEFAULT,
                                H5P_DEFAULT);
          }
        }

        mem = jsv->FindMember("NX_class");
        if (mem != jsv->MemberEnd()) {
          auto &nx_class = mem->value;
          if (nx_class.IsString()) {
            write_attribute_str(se.nxv, "NX_class", nx_class.GetString());
          }
        }

        mem = jsv->FindMember("NX_attributes");
        if (mem != jsv->MemberEnd()) {
          auto &a = mem->value;
          if (a.IsObject()) {
            for (auto &at : a.GetObject()) {
              if (at.value.IsString()) {
                write_attribute_str(se.nxv, at.name.GetString(),
                                    at.value.GetString());
              }
              if (at.value.IsInt64()) {
                write_attribute(se.nxv, at.name.GetString(),
                                at.value.GetInt64());
              }
              if (at.value.IsDouble()) {
                write_attribute(se.nxv, at.name.GetString(),
                                at.value.GetDouble());
              }
            }
          }
        }

        // TODO
        // Check if there are NX_attributes and set them

        se.basics = true;
      }

      bool have_child = false;
      if (se.jsv->IsObject()) {
        while (!have_child && se.itr != se.jsv->MemberEnd()) {
          if (strncmp("NX_", se.itr->name.GetString(), 3) != 0) {
            if (se.itr->value.IsObject()) {
              stack.emplace_back(se.itr->name.GetString(), &se.itr->value,
                                 se.nxv);
              have_child = true;
            } else if (se.itr->value.IsString()) {
              // Treated as a basic scalar string "nexus field" aka "hdf
              // dataset"
              string s1 = se.itr->value.GetString();
              auto dsp = H5Screate(H5S_SCALAR);
              H5Tset_size(strfix, s1.size());
              auto ds = H5Dcreate2(se.nxv, se.itr->name.GetString(), strfix,
                                   dsp, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
              H5Dwrite(ds, strfix, H5S_ALL, H5S_ALL, H5P_DEFAULT, s1.data());
              H5Dclose(ds);
              H5Sclose(dsp);
            } else if (se.itr->value.IsInt64()) {
              int64_t val = se.itr->value.GetInt64();
              auto dsp = H5Screate(H5S_SCALAR);
              auto ds =
                  H5Dcreate2(se.nxv, se.itr->name.GetString(), H5T_NATIVE_INT64,
                             dsp, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
              H5Dwrite(ds, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                       &val);
              H5Dclose(ds);
              H5Sclose(dsp);
            } else if (se.itr->value.IsDouble()) {
              double val = se.itr->value.GetDouble();
              auto dsp = H5Screate(H5S_SCALAR);
              auto ds = H5Dcreate2(se.nxv, se.itr->name.GetString(),
                                   H5T_NATIVE_DOUBLE, dsp, H5P_DEFAULT,
                                   H5P_DEFAULT, H5P_DEFAULT);
              H5Dwrite(ds, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                       &val);
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

  {
    using namespace date;
    using namespace std::chrono;
    auto now =
        make_zoned(current_zone(), floor<milliseconds>(system_clock::now()));
    write_hdf_iso8601(f1, "file_time", now);
  }

  H5Sclose(dsp_sc);
  H5Pclose(lcpl);
  H5Pclose(acpl);

  return 0;
}

void HDFFile::flush() { H5Fflush(impl->h5file, H5F_SCOPE_LOCAL); }

HDFFile_h5 HDFFile::h5file_detail() { return HDFFile_h5(impl->h5file); }

HDFFile_h5::HDFFile_h5(hid_t h5file) : _h5file(h5file) {}

hid_t HDFFile_h5::h5file() { return _h5file; }

std::unique_ptr<FBSchemaReader> FBSchemaReader::create(Msg msg) {
  static_assert(FLATBUFFERS_LITTLEENDIAN, "Requires currently little endian");
  if (msg.size < 8) {
    LOG(4, "ERROR message is too small");
    return nullptr;
  }
  Schemas::FBID fbid;
  memcpy(&fbid, msg.data + 4, 4);
  if (auto &cr = Schemas::SchemaRegistry::find(fbid)) {
    return cr->create_reader();
  }
  LOG(5, "does not seem like a known schema id: {:0x}  {:4.4s}",
      uint32_t((uint64_t)fbid.data()), (char *)fbid.data());
  return nullptr;
}

std::unique_ptr<FBSchemaWriter> FBSchemaReader::create_writer() {
  return create_writer_impl();
}

FBSchemaReader::~FBSchemaReader() {}

bool FBSchemaReader::verify(Msg msg) { return verify_impl(msg); }

bool FBSchemaReader::verify_impl(Msg msg) { return false; }

std::string FBSchemaReader::sourcename(Msg msg) { return sourcename_impl(msg); }

uint64_t FBSchemaReader::ts(Msg msg) { return ts_impl(msg); }

uint64_t FBSchemaReader::teamid(Msg &msg) { return teamid_impl(msg); }

uint64_t FBSchemaReader::teamid_impl(Msg &msg) { return 0; }

FBSchemaWriter::FBSchemaWriter() {}

FBSchemaWriter::~FBSchemaWriter() {
  if (group_event_data != -1)
    H5Gclose(group_event_data);
}

void FBSchemaWriter::init(HDFFile *hdf_file, std::string const &hdf_path,
                          std::string const &sourcename, Msg msg,
                          rapidjson::Value const *config_file) {
  this->config_file = config_file;
  this->hdf_file = hdf_file;
  auto f1 = hdf_file->h5file_detail().h5file();

  // if it proves useful, factor out
  H5E_auto2_t _f;
  void *_d;
  H5Eget_auto2(H5E_DEFAULT, &_f, &_d);
  H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
  hid_t gid = H5Gopen2(f1, hdf_path.c_str(), H5P_DEFAULT);
  if (gid < 0) {
    LOG(2, "can not find group {}, using file root instead", hdf_path);
    gid = f1;
  }
  H5Eset_auto2(H5E_DEFAULT, _f, _d);

  init_impl(sourcename, gid, msg);
}

WriteResult FBSchemaWriter::write(Msg msg) { return write_impl(msg); }

} // namespace FileWriter
} // namespace BrightnESS
