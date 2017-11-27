#include "HDFFile.h"
#include "HDFFile_h5.h"
#include "HDFFile_impl.h"
#include "date/date.h"
#include "h5.h"
#include "helper.h"
#include "json.h"
#include "logger.h"
#include <array>
#include <chrono>
#include <ctime>
#include <deque>
#include <flatbuffers/flatbuffers.h>
#include <hdf5.h>
#include <stack>
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
}

HDFFile::~HDFFile() {
  if (h5file >= 0) {
    std::array<char, 512> fname;
    H5Fget_name(h5file, fname.data(), fname.size());
    LOG(Sev::Debug, "flush file {}", fname.data());
    H5Fflush(h5file, H5F_SCOPE_LOCAL);
    LOG(Sev::Debug, "close file {}", fname.data());
    H5Fclose(h5file);
  }
}

static void write_hdf_ds_scalar_string(hid_t loc, std::string name,
                                       std::string s1) {
  auto strfix = H5Tcopy(H5T_C_S1);
  H5Tset_cset(strfix, H5T_CSET_UTF8);
  H5Tset_size(strfix, s1.size());
  using A = std::array<hsize_t, 1>;
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
static void write_hdf_iso8601(hid_t loc, const std::string &name, T &ts) {
  using namespace date;
  using namespace std::chrono;
  auto s2 = format("%Y-%m-%dT%H:%M:%S%z", ts);
  write_hdf_ds_scalar_string(loc, name, s2);
}

static void write_hdf_iso8601_now(hid_t location, const std::string &name) {
  using namespace date;
  using namespace std::chrono;
  const time_zone *current_time_zone;
  try {
    current_time_zone = current_zone();
  } catch (std::runtime_error &) {
    LOG(Sev::Warning, "ERROR failed to detect time zone for use in ISO8601 "
                      "timestamp in HDF file")
    return;
  }
  auto now =
      make_zoned(current_time_zone, floor<milliseconds>(system_clock::now()));
  write_hdf_iso8601(location, name, now);
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

template <typename T>
static void write_attribute(hid_t loc, std::string name, T value) {
  auto acpl = H5Pcreate(H5P_ATTRIBUTE_CREATE);
  H5Pset_char_encoding(acpl, H5T_CSET_UTF8);
  auto dsp_sc = H5Screate(H5S_SCALAR);
  auto at = H5Acreate2(loc, name.c_str(), h5::nat_type<T>(), dsp_sc, acpl,
                       H5P_DEFAULT);
  H5Awrite(at, h5::nat_type<T>(), &value);
  H5Aclose(at);
  H5Sclose(dsp_sc);
  H5Pclose(acpl);
}

template <typename T>
static void write_hdf_iso8601(hid_t loc, std::string name, T &ts) {
  using namespace date;
  using namespace std::chrono;
  auto s2 = format("%Y-%m-%dT%H:%M:%S%z", ts);
  write_attribute_str(loc, name, s2.data());
}

struct SE {
  string name;
  rapidjson::Value const *jsv;
  hid_t nxparent;
  hid_t nxv;
  hid_t gid;
  rapidjson::Value::ConstMemberIterator itr;
  bool basics;
  string nx_type{"group"};
  SE(string name, rapidjson::Value const *jsv, hid_t nxparent)
      : name(std::move(name)), jsv(jsv), nxparent(nxparent), nxv(-1), gid(-1),
        itr(jsv->MemberEnd()), basics(false) {
    if (jsv->IsObject()) {
      itr = jsv->MemberBegin();
    }
  }
  ~SE() {
    if (gid != -1) {
      H5Gclose(gid);
    }
  }
};

void write_attributes(hid_t hdf_this, rapidjson::Value const *jsv) {
  if (jsv->IsObject()) {
    for (auto &at : jsv->GetObject()) {
      if (at.value.IsString()) {
        write_attribute_str(hdf_this, at.name.GetString(),
                            at.value.GetString());
      }
      if (at.value.IsInt64()) {
        write_attribute(hdf_this, at.name.GetString(), at.value.GetInt64());
      }
      if (at.value.IsDouble()) {
        write_attribute(hdf_this, at.name.GetString(), at.value.GetDouble());
      }
    }
  }
}

void write_attributes_if_present(hid_t hdf_this, rapidjson::Value const *jsv) {
  auto mem = jsv->FindMember("attributes");
  if (mem != jsv->MemberEnd()) {
    write_attributes(hdf_this, &mem->value);
  }
}

template <typename DT>
static void populate_blob(std::vector<DT> &blob, rapidjson::Value const *vals) {
  if (vals->IsInt()) {
    blob.push_back(vals->GetInt());
  } else if (vals->IsDouble()) {
    blob.push_back(vals->GetDouble());
  } else if (vals->IsArray()) {
    std::stack<rapidjson::Value const *> as;
    std::stack<size_t> ai;
    std::stack<size_t> an;
    as.push(vals);
    ai.push(0);
    an.push(vals->GetArray().Size());

    while (not as.empty()) {
      if (as.size() > 10) {
        break;
      }
      // LOG(3, "level: {}  ai: {}  an: {}", as.size(), ai.back(), an.back());
      if (ai.top() >= an.top()) {
        as.pop();
        ai.pop();
        an.pop();
        continue;
      }
      auto &v = as.top()->GetArray()[ai.top()];
      if (v.IsArray()) {
        ai.top()++;
        as.push(&v);
        ai.push(0);
        size_t n = v.GetArray().Size();
        an.push(n);
      } else if (v.IsInt()) {
        blob.push_back((DT)v.GetInt());
        ai.top()++;
      } else if (v.IsInt64()) {
        blob.push_back((DT)v.GetInt64());
        ai.top()++;
      } else if (v.IsUint64()) {
        blob.push_back((DT)v.GetUint64());
        ai.top()++;
      } else if (v.IsDouble()) {
        blob.push_back((DT)v.GetDouble());
        ai.top()++;
      }
    }
  }
}

static void populate_string_pointers(std::vector<char const *> &ptrs,
                                     rapidjson::Value const *vals) {
  if (vals->IsString()) {
    ptrs.push_back(vals->GetString());
  } else if (vals->IsArray()) {
    std::stack<rapidjson::Value const *> as;
    std::stack<size_t> ai;
    std::stack<size_t> an;
    as.push(vals);
    ai.push(0);
    an.push(vals->GetArray().Size());

    while (not as.empty()) {
      if (as.size() > 10) {
        break;
      }
      if (ai.top() >= an.top()) {
        as.pop();
        ai.pop();
        an.pop();
        continue;
      }
      auto &v = as.top()->GetArray()[ai.top()];
      if (v.IsArray()) {
        ai.top()++;
        as.push(&v);
        ai.push(0);
        size_t n = v.GetArray().Size();
        an.push(n);
      } else if (v.IsString()) {
        ptrs.push_back(v.GetString());
        ai.top()++;
      }
    }
  }
}

static void populate_string_fixed_size(std::vector<char> &blob,
                                       hsize_t element_size,
                                       rapidjson::Value const *vals) {
  size_t n_added = 0;
  if (vals->IsString()) {
    std::copy(vals->GetString(), vals->GetString() + vals->GetStringLength(),
              blob.data() + n_added * element_size);
    ++n_added;
  } else if (vals->IsArray()) {
    std::stack<rapidjson::Value const *> as;
    std::stack<size_t> ai;
    std::stack<size_t> an;
    as.push(vals);
    ai.push(0);
    an.push(vals->GetArray().Size());

    while (not as.empty()) {
      if (as.size() > 10) {
        break;
      }
      if (ai.top() >= an.top()) {
        as.pop();
        ai.pop();
        an.pop();
        continue;
      }
      auto &v = as.top()->GetArray()[ai.top()];
      if (v.IsArray()) {
        ai.top()++;
        as.push(&v);
        ai.push(0);
        size_t n = v.GetArray().Size();
        an.push(n);
      } else if (v.IsString()) {
        std::copy(v.GetString(), v.GetString() + v.GetStringLength(),
                  blob.data() + n_added * element_size);
        ++n_added;
        ai.top()++;
      }
    }
  }
}

template <typename DT>
static void
write_ds_numeric(hid_t hdf_parent, std::string name, std::vector<hsize_t> sizes,
                 std::vector<hsize_t> max, rapidjson::Value const *vals) {
  size_t total_n = 1;
  for (auto x : sizes) {
    total_n *= x;
  }
  auto dcpl = H5Pcreate(H5P_DATASET_CREATE);
  hid_t dsp = -1;
  if (sizes.empty()) {
    dsp = H5Screate(H5S_SCALAR);
  } else {
    dsp = H5Screate(H5S_SIMPLE);
    H5Sset_extent_simple(dsp, static_cast<int>(sizes.size()), sizes.data(),
                         max.data());
    if (max[0] == H5S_UNLIMITED) {
      H5Pset_chunk(dcpl, static_cast<int>(sizes.size()), sizes.data());
    }
  }

  std::vector<DT> blob;
  populate_blob(blob, vals);

  if (blob.size() != total_n) {
    LOG(Sev::Error,
        "unexpected number of values for dataset {}  expected: {}  actual: {}",
        name, total_n, blob.size());
    H5Sclose(dsp);
    H5Pclose(dcpl);
    return;
  }

  auto dt = h5::nat_type<DT>();
  auto ds = H5Dcreate2(hdf_parent, name.data(), dt, dsp, H5P_DEFAULT, dcpl,
                       H5P_DEFAULT);
  auto err = H5Dwrite(ds, dt, H5S_ALL, H5S_ALL, H5P_DEFAULT, blob.data());
  if (err < 0) {
    LOG(Sev::Error, "error while writing dataset {}", name);
  }
  H5Dclose(ds);
  H5Sclose(dsp);
  H5Pclose(dcpl);
}

static void write_ds_string(hid_t hdf_parent, std::string name,
                            std::vector<hsize_t> sizes,
                            std::vector<hsize_t> max,
                            rapidjson::Value const *vals) {
  size_t total_n = 1;
  for (auto x : sizes) {
    total_n *= x;
  }
  auto dcpl = H5Pcreate(H5P_DATASET_CREATE);
  hid_t dsp = -1;
  if (sizes.empty()) {
    dsp = H5Screate(H5S_SCALAR);
  } else {
    dsp = H5Screate(H5S_SIMPLE);
    H5Sset_extent_simple(dsp, (int)sizes.size(), sizes.data(), max.data());
    if (max[0] == H5S_UNLIMITED) {
      H5Pset_chunk(dcpl, sizes.size(), sizes.data());
    }
  }

  std::vector<char const *> blob;
  populate_string_pointers(blob, vals);

  if (blob.size() != total_n) {
    LOG(Sev::Error,
        "unexpected number of values for dataset {}  expected: {}  actual: {}",
        name, total_n, blob.size());
    H5Sclose(dsp);
    H5Pclose(dcpl);
    return;
  }

  auto dt = H5Tcopy(H5T_C_S1);
  H5Tset_size(dt, H5T_VARIABLE);
  H5Tset_cset(dt, H5T_CSET_UTF8);

  auto ds = H5Dcreate2(hdf_parent, name.data(), dt, dsp, H5P_DEFAULT, dcpl,
                       H5P_DEFAULT);
  auto err = H5Dwrite(ds, dt, H5S_ALL, H5S_ALL, H5P_DEFAULT, blob.data());
  if (err < 0) {
    LOG(Sev::Error, "error while writing dataset {}", name);
  }
  H5Dclose(ds);
  H5Sclose(dsp);
  H5Pclose(dcpl);
  H5Tclose(dt);
}

static void write_ds_string_fixed_size(hid_t hdf_parent, std::string name,
                                       std::vector<hsize_t> sizes,
                                       std::vector<hsize_t> max,
                                       hsize_t element_size,
                                       rapidjson::Value const *vals) {
  size_t total_n = 1;
  for (auto x : sizes) {
    total_n *= x;
  }
  auto dcpl = H5Pcreate(H5P_DATASET_CREATE);
  hid_t dsp;
  if (sizes.empty()) {
    dsp = H5Screate(H5S_SCALAR);
  } else {
    dsp = H5Screate(H5S_SIMPLE);
    H5Sset_extent_simple(dsp, static_cast<int>(sizes.size()), sizes.data(),
                         max.data());
    if (max[0] == H5S_UNLIMITED) {
      H5Pset_chunk(dcpl, static_cast<int>(sizes.size()), sizes.data());
    }
  }

  std::vector<char> blob;
  if (element_size < 1024 * 1024) {
    blob.resize(total_n * element_size);
  }
  populate_string_fixed_size(blob, element_size, vals);

  if (blob.size() != total_n * element_size) {
    LOG(Sev::Error, "error in sizes");
    H5Sclose(dsp);
    H5Pclose(dcpl);
    return;
  }

  auto dt = H5Tcopy(H5T_C_S1);
  H5Tset_size(dt, element_size);
  H5Tset_cset(dt, H5T_CSET_UTF8);

  auto ds = H5Dcreate2(hdf_parent, name.data(), dt, dsp, H5P_DEFAULT, dcpl,
                       H5P_DEFAULT);
  auto err = H5Dwrite(ds, dt, H5S_ALL, H5S_ALL, H5P_DEFAULT, blob.data());
  if (err < 0) {
    LOG(Sev::Error, "error while writing dataset");
  }
  H5Dclose(ds);
  H5Sclose(dsp);
  H5Pclose(dcpl);
  H5Tclose(dt);
}

static void write_ds_generic(std::string const &dtype, hid_t hdf_parent,
                             std::string const &name,
                             std::vector<hsize_t> const &sizes,
                             std::vector<hsize_t> const &max,
                             hsize_t element_size,
                             rapidjson::Value const *vals) {
  if (dtype == "uint8") {
    write_ds_numeric<uint8_t>(hdf_parent, name, sizes, max, vals);
  }
  if (dtype == "uint16") {
    write_ds_numeric<uint16_t>(hdf_parent, name, sizes, max, vals);
  }
  if (dtype == "uint32") {
    write_ds_numeric<uint32_t>(hdf_parent, name, sizes, max, vals);
  }
  if (dtype == "uint64") {
    write_ds_numeric<uint64_t>(hdf_parent, name, sizes, max, vals);
  }
  if (dtype == "int8") {
    write_ds_numeric<int8_t>(hdf_parent, name, sizes, max, vals);
  }
  if (dtype == "int16") {
    write_ds_numeric<int16_t>(hdf_parent, name, sizes, max, vals);
  }
  if (dtype == "int32") {
    write_ds_numeric<int32_t>(hdf_parent, name, sizes, max, vals);
  }
  if (dtype == "int64") {
    write_ds_numeric<int64_t>(hdf_parent, name, sizes, max, vals);
  }
  if (dtype == "float") {
    write_ds_numeric<float>(hdf_parent, name, sizes, max, vals);
  }
  if (dtype == "double") {
    write_ds_numeric<double>(hdf_parent, name, sizes, max, vals);
  }
  if (dtype == "string") {
    if (element_size == H5T_VARIABLE) {
      write_ds_string(hdf_parent, name, sizes, max, vals);
    } else {
      write_ds_string_fixed_size(hdf_parent, name, sizes, max, element_size,
                                 vals);
    }
  }
}

static void write_dataset(hid_t hdf_parent, rapidjson::Value const *value) {
  std::string name;
  if (auto x = get_string(value, "name")) {
    name = x.v;
  } else {
    return;
  }

  std::string dtype = "int64";
  hsize_t element_size = H5T_VARIABLE;

  std::vector<hsize_t> sizes;
  auto ds = get_object(*value, "dataset");
  if (ds) {
    auto dso = ds.v->GetObject();
    auto ds_space = get_string(ds.v, "space");
    if (ds_space) {
      if (ds_space.v != "simple") {
        LOG(Sev::Warning, "sorry, can only handle simple data spaces");
        return;
      }
    }

    auto ds_type = get_string(ds.v, "type");
    if (ds_type) {
      dtype = ds_type.v;
    }

    // optional, default to scalar
    auto ds_size = get_array(*ds.v, "size");
    if (ds_size) {
      auto a = ds_size.v->GetArray();
      for (size_t i1 = 0; i1 < a.Size(); ++i1) {
        if (a[i1].IsInt()) {
          sizes.push_back(a[i1].GetInt());
        } else if (a[i1].IsString()) {
          if (string("unlimited") == a[i1].GetString()) {
            sizes.push_back(H5S_UNLIMITED);
          }
        }
      }
    }

    if (auto x = get_int(ds.v, "string_size")) {
      if (x.v > 0 && x.v != H5T_VARIABLE) {
        element_size = x.v;
      }
    }
  }

  auto ds_values_it = value->FindMember("values");
  if (ds_values_it == value->MemberEnd()) {
    return;
  }
  auto ds_values = &ds_values_it->value;

  if (ds_values->IsDouble()) {
    dtype = "double";
  }

  auto max = sizes;
  if (not sizes.empty()) {
    if (sizes[0] == H5S_UNLIMITED) {
      if (ds_values->IsArray()) {
        sizes[0] = ds_values->GetArray().Size();
      } else {
        sizes[0] = 1;
      }
    }
  }

  auto vals = ds_values;
  write_ds_generic(dtype, hdf_parent, name, sizes, max, element_size, vals);

  // Handle attributes on this dataset
  if (auto x = get_object(*value, "attributes")) {
    auto dsid = H5Dopen2(hdf_parent, name.data(), H5P_DEFAULT);
    write_attributes(dsid, x.v);
    H5Dclose(dsid);
  }
}

static void create_hdf_structures(rapidjson::Value const *value,
                                  hid_t hdf_parent, uint16_t level, hid_t lcpl,
                                  hid_t hdf_type_strfix,
                                  std::vector<StreamHDFInfo> &stream_hdf_info,
                                  std::deque<std::string> &path) {
  // The HDF object that we will maybe create at the current level.
  hid_t hdf_this = -1;
  // Keeps the HDF object id if we create a new collection-like object which
  // can be used as the parent for the next level of recursion. The only case
  // currently is when we create a group.
  hid_t hdf_next_parent = -1;
  // Remember whether we created a group at this level.
  hid_t gid = -1;
  {
    if (auto type = get_string(value, "type")) {
      if (type.v == "group") {
        if (auto name = get_string(value, "name")) {
          hdf_this = H5Gcreate2(hdf_parent, name.v.c_str(), lcpl, H5P_DEFAULT,
                                H5P_DEFAULT);
          hdf_next_parent = hdf_this;
          path.push_back(name.v);
          gid = hdf_this;
        }
      }
      if (type.v == "stream") {
        string pathstr = "/";
        for (auto &x : path) {
          pathstr += "/" + x;
        }
        stream_hdf_info.push_back(StreamHDFInfo{pathstr, value});
      }
      if (type.v == "dataset") {
        write_dataset(hdf_parent, value);
      }
    }
  }

  if (hdf_this >= 0) {
    write_attributes_if_present(hdf_this, value);
  }

  // If the current level in the HDF can act as a parent, then continue the
  // recursion with the (optional) "children" array.
  if (hdf_next_parent >= 0) {
    auto mem = value->FindMember("children");
    if (mem != value->MemberEnd()) {
      if (mem->value.IsArray()) {
        for (auto &child : mem->value.GetArray()) {
          create_hdf_structures(&child, hdf_this, level + 1, lcpl,
                                hdf_type_strfix, stream_hdf_info, path);
        }
      }
    }
    path.pop_back();
  }
  if (gid != -1) {
    H5Gclose(gid);
  }
}

/// Human readable version of the HDF5 headers that we compile against.
static std::string h5_version_string_headers_compile_time() {
  return fmt::format("{}.{}.{}", H5_VERS_MAJOR, H5_VERS_MINOR, H5_VERS_RELEASE);
}

/// Human readable version of the HDF5 libraries that we run with.
std::string h5_version_string_linked() {
  unsigned h5_vers_major, h5_vers_minor, h5_vers_release;
  H5get_libversion(&h5_vers_major, &h5_vers_minor, &h5_vers_release);
  return fmt::format("{}.{}.{}", h5_vers_major, h5_vers_minor, h5_vers_release);
}

/// Compare the version of the HDF5 headers which the kafka-to-nexus was
/// compiled with against the version of the HDF5 libraries that the
/// kafka-to-nexus is linked against at runtime. Currently, a mismatch in the
/// release number is logged but does not cause panic.
static void check_hdf_version() {
  unsigned h5_vers_major, h5_vers_minor, h5_vers_release;
  H5get_libversion(&h5_vers_major, &h5_vers_minor, &h5_vers_release);
  if (h5_vers_major != H5_VERS_MAJOR) {
    LOG(Sev::Error, "HDF5 version mismatch.  compile time: {}  runtime: {}",
        h5_version_string_headers_compile_time(), h5_version_string_linked());
    exit(1);
  }
  if (h5_vers_minor != H5_VERS_MINOR) {
    LOG(Sev::Error, "HDF5 version mismatch.  compile time: {}  runtime: {}",
        h5_version_string_headers_compile_time(), h5_version_string_linked());
    exit(1);
  }
  if (h5_vers_release != H5_VERS_RELEASE) {
    LOG(Sev::Error, "HDF5 version mismatch.  compile time: {}  runtime: {}",
        h5_version_string_headers_compile_time(), h5_version_string_linked());
  }
}

extern "C" char const GIT_COMMIT[];

int HDFFile::init(std::string filename, rapidjson::Value const &nexus_structure,
                  std::vector<StreamHDFInfo> &stream_hdf_info) {
  using std::string;
  using std::vector;
  using rapidjson::Value;
  auto x = H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
  if (x < 0) {
    std::array<char, 256> cwd;
    getcwd(cwd.data(), cwd.size());
    LOG(Sev::Error, "ERROR could not create the HDF file: {}  cwd: {}",
        filename, cwd.data());
    return -1;
  }
  h5file = x;
  return init(h5file, filename, nexus_structure, stream_hdf_info);
}

int HDFFile::init(hid_t h5file, std::string filename,
                  rapidjson::Value const &nexus_structure,
                  std::vector<StreamHDFInfo> &stream_hdf_info) {
  auto lcpl = H5Pcreate(H5P_LINK_CREATE);
  H5Pset_char_encoding(lcpl, H5T_CSET_UTF8);
  auto acpl = H5Pcreate(H5P_ATTRIBUTE_CREATE);
  H5Pset_char_encoding(acpl, H5T_CSET_UTF8);
  auto strfix = H5Tcopy(H5T_C_S1);
  H5Tset_cset(strfix, H5T_CSET_UTF8);
  H5Tset_size(strfix, 1);
  auto dsp_sc = H5Screate(H5S_SCALAR);
  check_hdf_version();
  write_attribute_str(h5file, "HDF5_Version",
                      h5_version_string_linked().data());
  write_attribute_str(h5file, "file_name", filename.data());
  write_attribute_str(
      h5file, "creator",
      fmt::format("kafka-to-nexus commit {:.7}", GIT_COMMIT).data());
  write_hdf_iso8601_now(h5file, "file_time");

  write_attributes_if_present(h5file, &nexus_structure);

  std::deque<std::string> path;
  if (nexus_structure.IsObject()) {
    auto value = &nexus_structure;
    auto mem = value->FindMember("children");
    if (mem != value->MemberEnd()) {
      if (mem->value.IsArray()) {
        for (auto &child : mem->value.GetArray()) {
          create_hdf_structures(&child, h5file, 0, lcpl, strfix,
                                stream_hdf_info, path);
        }
      }
    }
  }

  H5Sclose(dsp_sc);
  H5Pclose(lcpl);
  H5Pclose(acpl);

  return 0;
}

void HDFFile::flush() { H5Fflush(h5file, H5F_SCOPE_LOCAL); }

HDFFile_h5::HDFFile_h5(hid_t h5file) : _h5file(h5file) {}

hid_t HDFFile_h5::h5file() { return _h5file; }

} // namespace FileWriter
