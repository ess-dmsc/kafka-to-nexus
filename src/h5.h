#pragma once
#include <array>
#include <hdf5.h>
#include <mapbox/variant.hpp>
#include <memory>
#include <string>
#include <vector>

namespace h5 {

using std::unique_ptr;
using std::move;
using std::array;
using std::vector;
using std::string;

namespace h5p {

using mapbox::util::variant;

class dataset_create {
public:
  typedef unique_ptr<dataset_create> ptr;
  static ptr chunked1(hid_t type, hsize_t bytes);
  static dataset_create chunked1_nocheck(hid_t type, hsize_t bytes);
  static variant<bool, dataset_create> chunked1_var(hid_t type, hsize_t bytes);
  static dataset_create chunked1_or_exc(hid_t type, hsize_t bytes);
  static ptr chunked2(hid_t type, hsize_t ncols, hsize_t bytes);
  dataset_create(dataset_create &&x);
  ~dataset_create();
  friend void swap(dataset_create &x, dataset_create &y);
  hid_t id = -1;

private:
  dataset_create();
  dataset_create(dataset_create const &x);
};

} // namespace h5p

class h5d;

class h5s {
public:
  typedef unique_ptr<h5s> ptr;
  template <size_t N> static ptr simple_unlim(array<hsize_t, N> const &sini);
  h5s(h5d const &x);
  h5s(h5s &&x);
  ~h5s();
  friend void swap(h5s &x, h5s &y);
  hid_t id = -1;
  vector<hsize_t> sini;
  vector<hsize_t> smax;

private:
  h5s();
};

struct append_ret {
  int status;
  uint64_t written_bytes;
  uint64_t ix0;
  operator bool() const { return status == 0; }
};

class h5d {
public:
  typedef unique_ptr<h5d> ptr;
  static ptr create(hid_t loc, string name, hid_t type, h5s dsp,
                    h5p::dataset_create dcpl);
  h5d(h5d &&x);
  ~h5d();
  friend void swap(h5d &x, h5d &y);
  template <typename T> append_ret append_data_1d(T const *data, hsize_t nlen);
  template <typename T> append_ret append_data_2d(T const *data, hsize_t nlen);
  hid_t id = -1;
  hid_t type = -1;

private:
  h5d();
};

template <typename T> class h5d_chunked_1d {
public:
  typedef unique_ptr<h5d_chunked_1d<T>> ptr;
  static ptr create(hid_t loc, string name, hsize_t chunk_bytes);
  h5d ds;
  h5d_chunked_1d(h5d_chunked_1d &&x);
  ~h5d_chunked_1d();
  friend void swap(h5d_chunked_1d &x, h5d_chunked_1d &y);
  append_ret append_data_1d(T const *data, hsize_t nlen);
  int flush_buf();

private:
  h5d_chunked_1d();
  h5d_chunked_1d(hid_t loc, string name, hsize_t chunk_bytes, h5d ds);
  h5s dsp_wr;
  std::vector<T> buf;
  hsize_t i0 = 0;
};

template <typename T> class h5d_chunked_2d {
public:
  typedef unique_ptr<h5d_chunked_2d<T>> ptr;
  static ptr create(hid_t loc, string name, hsize_t ncols, hsize_t chunk_bytes);
  h5d ds;
  h5d_chunked_2d(h5d_chunked_2d &&x);
  ~h5d_chunked_2d();
  friend void swap(h5d_chunked_2d &x, h5d_chunked_2d &y);
  append_ret append_data_2d(T const *data, hsize_t nlen);
  int flush_buf();

private:
  h5d_chunked_2d();
  h5d_chunked_2d(hid_t loc, string name, hsize_t ncols, hsize_t chunk_bytes,
                 h5d ds);
  h5s dsp_wr;
  hsize_t ncols;
  std::vector<T> buf;
  uint32_t buf_bytes = 0;
  hsize_t i0 = 0;
};

} // namespace h5
