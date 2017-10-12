#include "HDFFile.h"
#include "HDFFile_h5.h"
#include "HDFFile_impl.h"
#include "date/date.h"
#include "helper.h"
#include "json.h"
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
    LOG(6, "flush file {}", fname.data());
    H5Fflush(impl->h5file, H5F_SCOPE_LOCAL);
    LOG(6, "close file {}", fname.data());
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
  A sini{{1}};
  A smax{{1}};
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

static void create_hdf_structures(rapidjson::Value const *value,
                                  hid_t hdf_parent, uint16_t level, hid_t lcpl,
                                  std::vector<StreamHDFInfo> &stream_hdf_info) {
  // The HDF object that we will maybe create at the current level.
  hid_t hdf_this = -1;
  // Keeps the HDF object id if we create a new collection-like object
  // which
  // can be used as the parent for the next level of recursion. The only
  // case
  // currently is when we create a group.
  hid_t hdf_next_parent = -1;
  {
    if (auto type = get_string(value, "type")) {
      if (type.v == "group") {
        if (auto name = get_string(value, "name")) {
          hdf_this = H5Gcreate2(hdf_parent, name.v.c_str(), lcpl, H5P_DEFAULT,
                                H5P_DEFAULT);
          hdf_next_parent = hdf_this;
        }
      }
      if (type.v == "stream") {
        stream_hdf_info.push_back(StreamHDFInfo{hdf_parent, value});
      }
      if (type.v == "dataset") {
        LOG(3, "DATASET not yet implemented");
      }
    }
  }

  if (hdf_this >= 0) {
    auto attributes_member = value->FindMember("attributes");
    if (attributes_member != value->MemberEnd()) {
      auto &attributes = attributes_member->value;
      if (attributes.IsObject()) {
        for (auto &attr : attributes.GetObject()) {
          if (attr.value.IsString()) {
            write_attribute_str(hdf_this, attr.name.GetString(),
                                attr.value.GetString());
          }
          if (attr.value.IsInt64()) {
            write_attribute(hdf_this, attr.name.GetString(),
                            attr.value.GetInt64());
          }
          if (attr.value.IsDouble()) {
            write_attribute(hdf_this, attr.name.GetString(),
                            attr.value.GetDouble());
          }
        }
      }
    }
  }

  // If the current level in the HDF can act as a parent, then continue
  // the
  // recursion with the (optional) "children" array.
  if (hdf_next_parent >= 0) {
    auto mem = value->FindMember("children");
    if (mem != value->MemberEnd()) {
      if (mem->value.IsArray()) {
        for (auto &child : mem->value.GetArray()) {
          create_hdf_structures(&child, hdf_this, level + 1, lcpl,
                                stream_hdf_info);
        }
      }
    }
  }
}

int HDFFile::init(std::string filename, rapidjson::Value const &nexus_structure,
                  std::vector<StreamHDFInfo> &stream_hdf_info) {
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
    auto value = &nexus_structure;
    auto mem = value->FindMember("children");
    if (mem != value->MemberEnd()) {
      if (mem->value.IsArray()) {
        for (auto &child : mem->value.GetArray()) {
          create_hdf_structures(&child, impl->h5file, 0, lcpl, stream_hdf_info);
        }
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

} // namespace FileWriter
