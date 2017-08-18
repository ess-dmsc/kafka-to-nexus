#include "h5.h"
#include "logger.h"

namespace h5 {

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

void swap(hsize_t &x, hsize_t &y) {
  x ^= y;
  y ^= x;
  x ^= y;
}

namespace h5p {

dataset_create::ptr dataset_create::chunked1(hid_t type, hsize_t bytes) {
  auto id = H5Pcreate(H5P_DATASET_CREATE);
  if (id == -1) {
    return {nullptr};
  }
  array<hsize_t, 1> schk{{std::max<hsize_t>(bytes / H5Tget_size(type), 1)}};
  H5Pset_chunk(id, schk.size(), schk.data());
  auto ret = ptr(new dataset_create);
  ret->id = id;
  return ret;
}

dataset_create::ptr dataset_create::chunked2(hid_t type, hsize_t ncols,
                                             hsize_t bytes) {
  auto id = H5Pcreate(H5P_DATASET_CREATE);
  if (id == -1) {
    return {nullptr};
  }
  array<hsize_t, 2> schk{
      {std::max<hsize_t>(bytes / ncols / H5Tget_size(type), 1), ncols}};
  H5Pset_chunk(id, schk.size(), schk.data());
  auto ret = ptr(new dataset_create);
  ret->id = id;
  return ret;
}

dataset_create::dataset_create(dataset_create const &x)
    : id(H5Iinc_ref(x.id)) {}

dataset_create::dataset_create(dataset_create &&x) {
  using std::swap;
  swap(*this, x);
}

dataset_create::~dataset_create() {
  if (id != -1) {
    H5Pclose(id);
  }
}

dataset_create::dataset_create() {}

void swap(dataset_create &x, dataset_create &y) {
  using std::swap;
  swap(x.id, y.id);
}

} // namespace h5p

template <size_t N> h5s::ptr h5s::simple_unlim(array<hsize_t, N> const &sini) {
  auto ret = ptr(new h5s);
  auto &o = *ret;
  o.sini = {sini.data(), sini.data() + sini.size()};
  o.smax.clear();
  o.smax.resize(o.sini.size(), H5S_UNLIMITED);
  for (size_t i1 = 1; i1 < o.sini.size(); ++i1) {
    o.smax[i1] = o.sini[i1];
  }
  o.id = H5Screate_simple(o.sini.size(), o.sini.data(), o.smax.data());
  if (o.id == -1) {
    ret.reset();
  }
  return ret;
}

h5s::h5s(h5d const &x) {
  id = H5Dget_space(x.id);
  if (H5Sget_simple_extent_type(id) != H5S_SIMPLE) {
    throw std::runtime_error("expect a simple dataspace");
  }
  auto n = H5Sget_simple_extent_ndims(id);
  sini.resize(n);
  smax.resize(n);
  H5Sget_simple_extent_dims(id, sini.data(), smax.data());
}

void swap(h5s &x, h5s &y) {
  using std::swap;
  swap(x.id, y.id);
}

h5s::h5s() {}

h5s::h5s(h5s &&x) { swap(*this, x); }

h5s::~h5s() {
  if (id != -1) {
    H5Sclose(id);
  }
}

h5d::ptr h5d::create(hid_t loc, string name, hid_t type, h5s dsp,
                     h5p::dataset_create dcpl) {
  auto ret = ptr(new h5d);
  auto &o = *ret;
  o.type = type;
  o.id = H5Dcreate1(loc, name.c_str(), type, dsp.id, dcpl.id);
  if (o.id == -1) {
    ret.reset();
  }
  return ret;
}

h5d::h5d(h5d &&x) { swap(*this, x); }

h5d::~h5d() {
  if (id != -1)
    H5Dclose(id);
}

h5d::h5d() {}

void swap(h5d &x, h5d &y) {
  using std::swap;
  swap(x.id, y.id);
  swap(x.type, y.type);
}

template <typename T>
append_ret h5d::append_data_1d(T const *data, hsize_t nlen) {
  if (log_level >= 9) {
    array<char, 64> buf1;
    auto n1 = H5Iget_name(id, buf1.data(), buf1.size());
    if (n1 > 0) {
      LOG(9, "append_data_1d {} for dataset {:.{}}", nlen, buf1.data(), n1);
    }
  }
  auto tgt = H5Dget_space(id);
  // auto ndims = H5Sget_simple_extent_ndims(tgt);
  using A1 = array<hsize_t, 1>;
  A1 snow;
  A1 smax;
  herr_t err;
  H5Sget_simple_extent_dims(tgt, snow.data(), smax.data());
  if (log_level >= 9) {
    for (size_t i1 = 0; i1 < snow.size(); ++i1) {
      LOG(9, "H5Sget_simple_extent_dims {:3}", snow.at(i1));
    }
  }

  snow[0] += nlen;
  err = H5Dextend(id, snow.data());
  if (err < 0) {
    LOG(3, "can not extend dataset");
    H5Sclose(tgt);
    return {-1};
  }

  H5Sclose(tgt);

  tgt = H5Dget_space(id);
  A1 mem = {{nlen}};

  auto dsp_mem = H5Screate_simple(mem.size(), mem.data(), nullptr);
  {
    A1 start{{0}};
    A1 count{{nlen}};
    err = H5Sselect_hyperslab(dsp_mem, H5S_SELECT_SET, start.data(), nullptr,
                              count.data(), nullptr);
    if (err < 0) {
      LOG(3, "can not select mem hyperslab");
      return {-3};
    }
  }

  A1 tgt_start{{snow.at(0) - nlen}};
  A1 tgt_count{{nlen}};
  err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, tgt_start.data(), nullptr,
                            tgt_count.data(), nullptr);
  if (err < 0) {
    LOG(3, "can not select tgt hyperslab");
    return {-3};
  }

  err = H5Dwrite(id, type, dsp_mem, tgt, H5P_DEFAULT, data);
  if (err < 0) {
    LOG(3, "writing failed");
    return {-4};
  }
  return {0, sizeof(T) * nlen, tgt_start[0]};
}

template <typename T>
append_ret h5d::append_data_2d(T const *data, hsize_t nlen) {
  if (log_level >= 9) {
    array<char, 64> buf1;
    auto n1 = H5Iget_name(id, buf1.data(), buf1.size());
    if (n1 > 0) {
      LOG(9, "append_data_2d {} for dataset {:.{}}", nlen, buf1.data(), n1);
    }
  }
  auto tgt = H5Dget_space(id);
  uint8_t const NDIM = 2;
  using A1 = array<hsize_t, NDIM>;
  A1 snow;
  A1 smax;
  herr_t err;
  if (NDIM != H5Sget_simple_extent_ndims(tgt)) {
    LOG(3, "dataset dimensions do not match");
    return {-1};
  }
  H5Sget_simple_extent_dims(tgt, snow.data(), smax.data());
  if (log_level >= 9) {
    for (size_t i1 = 0; i1 < snow.size(); ++i1) {
      LOG(9, "snow {} {:3}", i1, snow.at(i1));
    }
  }

  hsize_t ncols = snow[1];
  if (nlen % ncols != 0) {
    LOG(3, "dataset dimensions do not match");
    return {-1};
  }

  hsize_t nrows = nlen / ncols;

  snow[0] += nrows;
  err = H5Dextend(id, snow.data());
  if (err < 0) {
    LOG(3, "can not extend dataset");
    H5Sclose(tgt);
    return {-1};
  }

  H5Sclose(tgt);

  tgt = H5Dget_space(id);
  A1 mem = {{nrows, ncols}};

  auto dsp_mem = H5Screate_simple(mem.size(), mem.data(), nullptr);
  {
    A1 start{{0, 0}};
    A1 count{{nrows, ncols}};
    err = H5Sselect_hyperslab(dsp_mem, H5S_SELECT_SET, start.data(), nullptr,
                              count.data(), nullptr);
    if (err < 0) {
      LOG(3, "can not select mem hyperslab");
      return {-3};
    }
  }

  A1 tgt_start{{snow[0] - nrows, 0}};
  A1 tgt_count{{nrows, ncols}};
  err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, tgt_start.data(), nullptr,
                            tgt_count.data(), nullptr);
  if (err < 0) {
    LOG(3, "can not select tgt hyperslab");
    return {-3};
  }

  err = H5Dwrite(id, type, dsp_mem, tgt, H5P_DEFAULT, data);
  if (err < 0) {
    LOG(3, "writing failed");
    return {-4};
  }
  return {0, sizeof(T) * nlen, tgt_start[0]};
}

template <typename T>
typename h5d_chunked_1d<T>::ptr
h5d_chunked_1d<T>::create(hid_t loc, string name, hsize_t chunk_bytes) {
  auto dsp = h5s::simple_unlim<1>({{0}});
  if (!dsp) {
    return nullptr;
  }
  auto dcpl = h5p::dataset_create::chunked1(nat_type<T>(), chunk_bytes);
  if (!dcpl) {
    return nullptr;
  }
  auto ds = h5d::create(loc, name, nat_type<T>(), move(*dsp), move(*dcpl));
  if (!ds) {
    return nullptr;
  }
  // todo: With these changes, return ::ptr directly.  Also in 2d case.
  auto ret = new h5d_chunked_1d<T>(loc, name, chunk_bytes, move(*ds));
  return ptr(ret);
}

template <typename T>
h5d_chunked_1d<T>::h5d_chunked_1d(hid_t loc, string name, hsize_t chunk_bytes,
                                  h5d ds)
    : ds(move(ds)), dsp_wr(this->ds) {
  buf.resize(buf_SIZE);
}

template <typename T>
h5d_chunked_1d<T>::h5d_chunked_1d(h5d_chunked_1d &&x)
    : ds(move(x.ds)), dsp_wr(move(x.dsp_wr)) {}

template <typename T> h5d_chunked_1d<T>::~h5d_chunked_1d() { flush_buf(); }

template <typename T> void swap(h5d_chunked_1d<T> &x, h5d_chunked_1d<T> &y) {
  swap(x.ds, y.ds);
  swap(x.dsp_wr, y.dsp_wr);
  swap(x.i0, y.i0);
}

template <typename T>
append_ret h5d_chunked_1d<T>::append_data_1d(T const *data, hsize_t nlen) {
  append_ret ret{-1};
  auto nbytes = nlen * sizeof(T);
  bool do_buf = nbytes <= buf_MAXPKG;
  if (do_buf) {
    auto p1 = (char *)data;
    auto p2 = buf.data() + buf_n;
    for (size_t i1 = 0; i1 < nbytes; ++i1) {
      p2[i1] = p1[i1];
    }
    buf_n += nbytes;
  }
  if (buf_n >= buf_SIZE - 2 * buf_MAXPKG || (!do_buf && buf_n > 0)) {
    if (flush_buf() != 0) {
      return {-1};
    }
  }
  if (!do_buf) {
    ret = ds.append_data_1d(data, nlen);
    if (!ret) {
      return ret;
    }
  }
  ret.status = 0;
  ret.ix0 = i0;
  ret.written_bytes = nbytes;
  i0 += nlen;
  return ret;
}

template <typename T> int h5d_chunked_1d<T>::flush_buf() {
  auto wr = ds.append_data_1d((T *)buf.data(), buf_n / sizeof(T));
  if (!wr) {
    return -1;
  }
  buf_n = 0;
  return 0;
}

template <typename T>
typename h5d_chunked_2d<T>::ptr
h5d_chunked_2d<T>::create(hid_t loc, string name, hsize_t ncols,
                          hsize_t chunk_bytes) {
  auto dsp = h5s::simple_unlim<2>({{0, ncols}});
  if (!dsp) {
    return nullptr;
  }
  auto dcpl = h5p::dataset_create::chunked2(nat_type<T>(), ncols, chunk_bytes);
  if (!dcpl) {
    return nullptr;
  }
  auto ds = h5d::create(loc, name, nat_type<T>(), move(*dsp), move(*dcpl));
  if (!ds) {
    return nullptr;
  }
  auto ret = new h5d_chunked_2d<T>(loc, name, ncols, chunk_bytes, move(*ds));
  return ptr(ret);
}

template <typename T>
h5d_chunked_2d<T>::h5d_chunked_2d(hid_t loc, string name, hsize_t ncols,
                                  hsize_t chunk_bytes, h5d ds)
    : ds(move(ds)), dsp_wr(this->ds), ncols(ncols) {}

template <typename T>
h5d_chunked_2d<T>::h5d_chunked_2d(h5d_chunked_2d &&x)
    : ds(move(x.ds)), dsp_wr(move(x.dsp_wr)) {}

template <typename T> h5d_chunked_2d<T>::~h5d_chunked_2d() { flush_buf(); }

template <typename T> void swap(h5d_chunked_2d<T> &x, h5d_chunked_2d<T> &y) {
  swap(x.ds, y.ds);
  swap(x.dsp_wr, y.dsp_wr);
  swap(x.i0, y.i0);
}

template <typename T>
append_ret h5d_chunked_2d<T>::append_data_2d(T const *data, hsize_t nlen) {
  append_ret ret{-1};
  if (nlen != dsp_wr.sini.at(1)) {
    return {-1};
  }
  bool do_buf = nlen * sizeof(T) < 4 * 1024;
  if (do_buf) {
    std::copy(data, data + nlen, std::back_inserter(buf));
    buf_bytes += nlen * sizeof(T);
  }
  if (buf_bytes > 128 * 1024 || (!do_buf && buf_bytes > 0)) {
    if (flush_buf() != 0) {
      return {-1};
    }
  }
  if (!do_buf) {
    ret = ds.append_data_2d(data, nlen);
    if (!ret) {
      return ret;
    }
  }
  ret.status = 0;
  ret.ix0 = i0;
  ret.written_bytes = sizeof(T) * nlen;
  i0 += nlen / dsp_wr.sini.at(1);
  return ret;
}

template <typename T> int h5d_chunked_2d<T>::flush_buf() {
  auto wr = ds.append_data_2d(buf.data(), buf.size());
  if (!wr) {
    return -1;
  }
  buf.clear();
  return 0;
}

template h5s::ptr h5s::simple_unlim(array<hsize_t, 1> const &sini);
template h5s::ptr h5s::simple_unlim(array<hsize_t, 2> const &sini);
template h5s::ptr h5s::simple_unlim(array<hsize_t, 3> const &sini);

template append_ret h5d::append_data_1d(uint8_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(uint16_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(uint32_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(uint64_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(int8_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(int16_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(int32_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(int64_t const *data, hsize_t nlen);

template h5d_chunked_1d<uint8_t>::ptr
h5d_chunked_1d<uint8_t>::create(hid_t loc, string name, hsize_t chunk_bytes);
template h5d_chunked_1d<uint16_t>::ptr
h5d_chunked_1d<uint16_t>::create(hid_t loc, string name, hsize_t chunk_bytes);
template h5d_chunked_1d<uint32_t>::ptr
h5d_chunked_1d<uint32_t>::create(hid_t loc, string name, hsize_t chunk_bytes);
template h5d_chunked_1d<uint64_t>::ptr
h5d_chunked_1d<uint64_t>::create(hid_t loc, string name, hsize_t chunk_bytes);
template h5d_chunked_1d<int8_t>::ptr
h5d_chunked_1d<int8_t>::create(hid_t loc, string name, hsize_t chunk_bytes);
template h5d_chunked_1d<int16_t>::ptr
h5d_chunked_1d<int16_t>::create(hid_t loc, string name, hsize_t chunk_bytes);
template h5d_chunked_1d<int32_t>::ptr
h5d_chunked_1d<int32_t>::create(hid_t loc, string name, hsize_t chunk_bytes);
template h5d_chunked_1d<int64_t>::ptr
h5d_chunked_1d<int64_t>::create(hid_t loc, string name, hsize_t chunk_bytes);
template h5d_chunked_1d<float>::ptr
h5d_chunked_1d<float>::create(hid_t loc, string name, hsize_t chunk_bytes);
template h5d_chunked_1d<double>::ptr
h5d_chunked_1d<double>::create(hid_t loc, string name, hsize_t chunk_bytes);

template h5d_chunked_1d<uint8_t>::h5d_chunked_1d(hid_t loc, string name,
                                                 hsize_t chunk_bytes, h5d ds);
template h5d_chunked_1d<uint16_t>::h5d_chunked_1d(hid_t loc, string name,
                                                  hsize_t chunk_bytes, h5d ds);
template h5d_chunked_1d<uint32_t>::h5d_chunked_1d(hid_t loc, string name,
                                                  hsize_t chunk_bytes, h5d ds);
template h5d_chunked_1d<uint64_t>::h5d_chunked_1d(hid_t loc, string name,
                                                  hsize_t chunk_bytes, h5d ds);
template h5d_chunked_1d<int8_t>::h5d_chunked_1d(hid_t loc, string name,
                                                hsize_t chunk_bytes, h5d ds);
template h5d_chunked_1d<int16_t>::h5d_chunked_1d(hid_t loc, string name,
                                                 hsize_t chunk_bytes, h5d ds);
template h5d_chunked_1d<int32_t>::h5d_chunked_1d(hid_t loc, string name,
                                                 hsize_t chunk_bytes, h5d ds);
template h5d_chunked_1d<int64_t>::h5d_chunked_1d(hid_t loc, string name,
                                                 hsize_t chunk_bytes, h5d ds);
template h5d_chunked_1d<float>::h5d_chunked_1d(hid_t loc, string name,
                                               hsize_t chunk_bytes, h5d ds);
template h5d_chunked_1d<double>::h5d_chunked_1d(hid_t loc, string name,
                                                hsize_t chunk_bytes, h5d ds);

template h5d_chunked_1d<uint8_t>::~h5d_chunked_1d();
template h5d_chunked_1d<uint16_t>::~h5d_chunked_1d();
template h5d_chunked_1d<uint32_t>::~h5d_chunked_1d();
template h5d_chunked_1d<uint64_t>::~h5d_chunked_1d();
template h5d_chunked_1d<int8_t>::~h5d_chunked_1d();
template h5d_chunked_1d<int16_t>::~h5d_chunked_1d();
template h5d_chunked_1d<int32_t>::~h5d_chunked_1d();
template h5d_chunked_1d<int64_t>::~h5d_chunked_1d();
template h5d_chunked_1d<float>::~h5d_chunked_1d();
template h5d_chunked_1d<double>::~h5d_chunked_1d();

template append_ret
h5d_chunked_1d<uint8_t>::h5d_chunked_1d::append_data_1d(uint8_t const *data,
                                                        hsize_t nlen);
template append_ret
h5d_chunked_1d<uint16_t>::h5d_chunked_1d::append_data_1d(uint16_t const *data,
                                                         hsize_t nlen);
template append_ret
h5d_chunked_1d<uint32_t>::h5d_chunked_1d::append_data_1d(uint32_t const *data,
                                                         hsize_t nlen);
template append_ret
h5d_chunked_1d<uint64_t>::h5d_chunked_1d::append_data_1d(uint64_t const *data,
                                                         hsize_t nlen);
template append_ret
h5d_chunked_1d<int8_t>::h5d_chunked_1d::append_data_1d(int8_t const *data,
                                                       hsize_t nlen);
template append_ret
h5d_chunked_1d<int16_t>::h5d_chunked_1d::append_data_1d(int16_t const *data,
                                                        hsize_t nlen);
template append_ret
h5d_chunked_1d<int32_t>::h5d_chunked_1d::append_data_1d(int32_t const *data,
                                                        hsize_t nlen);
template append_ret
h5d_chunked_1d<int64_t>::h5d_chunked_1d::append_data_1d(int64_t const *data,
                                                        hsize_t nlen);
template append_ret
h5d_chunked_1d<float>::h5d_chunked_1d::append_data_1d(float const *data,
                                                      hsize_t nlen);
template append_ret
h5d_chunked_1d<double>::h5d_chunked_1d::append_data_1d(double const *data,
                                                       hsize_t nlen);

template h5d_chunked_2d<uint8_t>::ptr
h5d_chunked_2d<uint8_t>::create(hid_t loc, string name, hsize_t ncols,
                                hsize_t chunk_bytes);
template h5d_chunked_2d<uint16_t>::ptr
h5d_chunked_2d<uint16_t>::create(hid_t loc, string name, hsize_t ncols,
                                 hsize_t chunk_bytes);
template h5d_chunked_2d<uint32_t>::ptr
h5d_chunked_2d<uint32_t>::create(hid_t loc, string name, hsize_t ncols,
                                 hsize_t chunk_bytes);
template h5d_chunked_2d<uint64_t>::ptr
h5d_chunked_2d<uint64_t>::create(hid_t loc, string name, hsize_t ncols,
                                 hsize_t chunk_bytes);
template h5d_chunked_2d<int8_t>::ptr
h5d_chunked_2d<int8_t>::create(hid_t loc, string name, hsize_t ncols,
                               hsize_t chunk_bytes);
template h5d_chunked_2d<int16_t>::ptr
h5d_chunked_2d<int16_t>::create(hid_t loc, string name, hsize_t ncols,
                                hsize_t chunk_bytes);
template h5d_chunked_2d<int32_t>::ptr
h5d_chunked_2d<int32_t>::create(hid_t loc, string name, hsize_t ncols,
                                hsize_t chunk_bytes);
template h5d_chunked_2d<int64_t>::ptr
h5d_chunked_2d<int64_t>::create(hid_t loc, string name, hsize_t ncols,
                                hsize_t chunk_bytes);
template h5d_chunked_2d<float>::ptr
h5d_chunked_2d<float>::create(hid_t loc, string name, hsize_t ncols,
                              hsize_t chunk_bytes);
template h5d_chunked_2d<double>::ptr
h5d_chunked_2d<double>::create(hid_t loc, string name, hsize_t ncols,
                               hsize_t chunk_bytes);

template h5d_chunked_2d<uint8_t>::h5d_chunked_2d(hid_t loc, string name,
                                                 hsize_t ncols,
                                                 hsize_t chunk_bytes, h5d ds);
template h5d_chunked_2d<uint16_t>::h5d_chunked_2d(hid_t loc, string name,
                                                  hsize_t ncols,
                                                  hsize_t chunk_bytes, h5d ds);
template h5d_chunked_2d<uint32_t>::h5d_chunked_2d(hid_t loc, string name,
                                                  hsize_t ncols,
                                                  hsize_t chunk_bytes, h5d ds);
template h5d_chunked_2d<uint64_t>::h5d_chunked_2d(hid_t loc, string name,
                                                  hsize_t ncols,
                                                  hsize_t chunk_bytes, h5d ds);
template h5d_chunked_2d<int8_t>::h5d_chunked_2d(hid_t loc, string name,
                                                hsize_t ncols,
                                                hsize_t chunk_bytes, h5d ds);
template h5d_chunked_2d<int16_t>::h5d_chunked_2d(hid_t loc, string name,
                                                 hsize_t ncols,
                                                 hsize_t chunk_bytes, h5d ds);
template h5d_chunked_2d<int32_t>::h5d_chunked_2d(hid_t loc, string name,
                                                 hsize_t ncols,
                                                 hsize_t chunk_bytes, h5d ds);
template h5d_chunked_2d<int64_t>::h5d_chunked_2d(hid_t loc, string name,
                                                 hsize_t ncols,
                                                 hsize_t chunk_bytes, h5d ds);
template h5d_chunked_2d<float>::h5d_chunked_2d(hid_t loc, string name,
                                               hsize_t ncols,
                                               hsize_t chunk_bytes, h5d ds);
template h5d_chunked_2d<double>::h5d_chunked_2d(hid_t loc, string name,
                                                hsize_t ncols,
                                                hsize_t chunk_bytes, h5d ds);

template h5d_chunked_2d<uint8_t>::~h5d_chunked_2d();
template h5d_chunked_2d<uint16_t>::~h5d_chunked_2d();
template h5d_chunked_2d<uint32_t>::~h5d_chunked_2d();
template h5d_chunked_2d<uint64_t>::~h5d_chunked_2d();
template h5d_chunked_2d<int8_t>::~h5d_chunked_2d();
template h5d_chunked_2d<int16_t>::~h5d_chunked_2d();
template h5d_chunked_2d<int32_t>::~h5d_chunked_2d();
template h5d_chunked_2d<int64_t>::~h5d_chunked_2d();
template h5d_chunked_2d<float>::~h5d_chunked_2d();
template h5d_chunked_2d<double>::~h5d_chunked_2d();

template append_ret
h5d_chunked_2d<uint8_t>::h5d_chunked_2d::append_data_2d(uint8_t const *data,
                                                        hsize_t nlen);
template append_ret
h5d_chunked_2d<uint16_t>::h5d_chunked_2d::append_data_2d(uint16_t const *data,
                                                         hsize_t nlen);
template append_ret
h5d_chunked_2d<uint32_t>::h5d_chunked_2d::append_data_2d(uint32_t const *data,
                                                         hsize_t nlen);
template append_ret
h5d_chunked_2d<uint64_t>::h5d_chunked_2d::append_data_2d(uint64_t const *data,
                                                         hsize_t nlen);
template append_ret
h5d_chunked_2d<int8_t>::h5d_chunked_2d::append_data_2d(int8_t const *data,
                                                       hsize_t nlen);
template append_ret
h5d_chunked_2d<int16_t>::h5d_chunked_2d::append_data_2d(int16_t const *data,
                                                        hsize_t nlen);
template append_ret
h5d_chunked_2d<int32_t>::h5d_chunked_2d::append_data_2d(int32_t const *data,
                                                        hsize_t nlen);
template append_ret
h5d_chunked_2d<int64_t>::h5d_chunked_2d::append_data_2d(int64_t const *data,
                                                        hsize_t nlen);
template append_ret
h5d_chunked_2d<float>::h5d_chunked_2d::append_data_2d(float const *data,
                                                      hsize_t nlen);
template append_ret
h5d_chunked_2d<double>::h5d_chunked_2d::append_data_2d(double const *data,
                                                       hsize_t nlen);

} // namespace h5
