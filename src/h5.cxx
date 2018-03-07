#include "h5.h"
#include "logger.h"
#include <chrono>

namespace h5 {

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

void h5d::init_basics() {
  herr_t err = 0;
  Type = hdf5::datatype::Datatype(hdf5::ObjectHandle(H5Dget_type(id)));
  dsp_tgt = H5Dget_space(id);
  ndims = H5Sget_simple_extent_ndims(dsp_tgt);
  snow = {{0, 0}};
  H5Sget_simple_extent_dims(dsp_tgt, sext.data(), smax.data());
  if (log_level >= 9) {
    for (size_t i1 = 0; i1 < ndims; ++i1) {
      LOG(Sev::Debug, "H5Sget_simple_extent_dims {:20} {}: {:21} {:21}", name,
          i1, sext.at(i1), smax.at(i1));
    }
  }
  try {
    DSPMem = hdf5::dataspace::Simple({0, 0}, {H5S_UNLIMITED, H5S_UNLIMITED});
  } catch (std::runtime_error const &e) {
    std::throw_with_nested(
        RuntimeError("hdf5::dataspace::Simple ctor failure"));
  }
  pl_transfer = H5Pcreate(H5P_DATASET_XFER);
  err = H5Pset_edc_check(pl_transfer, H5Z_DISABLE_EDC);
  if (err < 0) {
    LOG(Sev::Debug, "failed H5Pset_edc_check");
  }
}

h5d::ptr h5d::create(hid_t loc, string name, hid_t type,
                     hdf5::dataspace::Simple dsp,
                     hdf5::property::DatasetCreationList dcpl,
                     CollectiveQueue *cq) {
  // Creation is done in single process mode.
  // Can do set_extent here.
  auto ret = ptr(new h5d);
  auto &o = *ret;
  herr_t err = 0;
  o.Type = hdf5::datatype::Datatype(
      hdf5::ObjectHandle(type, hdf5::ObjectHandle::Policy::WITHOUT_WARD));
  o.name = name;
  err = H5Pset_fill_value(static_cast<hid_t>(dcpl), static_cast<hid_t>(o.Type),
                          nullptr);
  if (err < 0) {
    LOG(Sev::Debug, "failed H5Pset_fill_value");
  }
  o.id = H5Dcreate1(loc, name.c_str(), static_cast<hid_t>(o.Type),
                    static_cast<hid_t>(dsp), static_cast<hid_t>(dcpl));
  if (o.id < 0) {
    LOG(Sev::Error, "H5Dcreate1 failed");
    ret.reset();
    return ret;
  }
  o.init_basics();
  return ret;
}

h5d::ptr h5d::open_single(hid_t loc, string name, CollectiveQueue *cq,
                          HDFIDStore *hdf_store) {
  // Open in single-process mode
  auto ret = ptr(new h5d);
  auto &o = *ret;
  o.name = name;
  o.id = H5Dopen2(loc, name.c_str(), H5P_DEFAULT);
  if (o.id < 0) {
    LOG(Sev::Error, "H5Dopen2 failed");
    ret.reset();
    return ret;
  }
  o.init_basics();
  return ret;
}

h5d::ptr h5d::open(hid_t loc, string name, CollectiveQueue *cq,
                   HDFIDStore *hdf_store) {
  return open_single(loc, name, cq, hdf_store);
}

h5d::h5d(h5d &&x) { swap(*this, x); }

h5d::~h5d() {
  if (id != -1) {
    LOG(Sev::Debug, "~h5d ds");
    herr_t err = 0;
    char ds_name[512];
    auto &buf = ds_name;
    auto bufn = H5Iget_name(id, buf, 512);
    H5O_info_t oi;
    H5Oget_info(id, &oi);
    if (not cq) {
      err = H5Dset_extent(id, snow.data());
      if (err < 0) {
        LOG(Sev::Error, "H5Dset_extent failed");
      }
      err = H5Dclose(id);
      if (err < 0) {
        LOG(Sev::Error, "error closing dataset  {}  {:.{}}  H5Iget_ref: {}  "
                        "H5O_info_t.rc: {}",
            id, buf, bufn, H5Iget_ref(id), oi.rc);
      }
      id = -1;
    }
  }
  if (pl_transfer != -1) {
    H5Pclose(pl_transfer);
    pl_transfer = -1;
  }
}

h5d::h5d() {}

void swap(h5d &x, h5d &y) {
  using std::swap;
  swap(x.id, y.id);
  swap(x.name, y.name);
  swap(x.Type, y.Type);
  swap(x.pl_transfer, y.pl_transfer);
  swap(x.ndims, y.ndims);
  swap(x.DSPMem, y.DSPMem);
  swap(x.dsp_tgt, y.dsp_tgt);
  swap(x.snow, y.snow);
  swap(x.smax, y.smax);
  swap(x.sext, y.sext);
  swap(x.cq, y.cq);
  swap(x.hdf_store, y.hdf_store);
  swap(x.mpi_rank, y.mpi_rank);
}

void h5d::lookup_cqsnowix(char const *ds_name, size_t &cqsnowix) {
  LOG(Sev::Debug, "using cq: {}", (void *)cq);
}

template <typename T>
append_ret h5d::append_data_1d(T const *data, hsize_t nlen) {
  using namespace std::chrono;
  using CLK = steady_clock;
  using MS = std::chrono::milliseconds;
  auto t1 = CLK::now();
  LOG(Sev::Debug, "append_data_{}d", ndims);
  if (log_level >= 9) {
    array<char, 64> buf1;
    auto n1 = H5Iget_name(id, buf1.data(), buf1.size());
    if (n1 > 0) {
      LOG(Sev::Debug, "append_data_1d {} for dataset {:.{}}", nlen, buf1.data(),
          n1);
    }
  }
  using AT = array<hsize_t, 2>;
  herr_t err;

  char ds_name[512];
  {
    auto &buf = ds_name;
    auto bufn = H5Iget_name(id, buf, 512);
    buf[bufn] = '\0';
  }

  // TODO
  // Is there still a need to look up the DSP because some extent might have
  // happened in some other process? Yes, sure!!
  // Therefore, always lookup the dataspace from the hdf_store!

  if (false && log_level >= 9) {
    // Just for debugging
    AT snow, smax;
    err = H5Sget_simple_extent_dims(dsp_tgt, snow.data(), smax.data());
    if (err < 0) {
      LOG(Sev::Error, "failed H5Sget_simple_extent_dims");
    }
    for (size_t i1 = 0; i1 < ndims; ++i1) {
      LOG(Sev::Debug, "H5Sget_simple_extent_dims {} {:3} / {:3}", i1,
          snow.at(i1), smax.at(i1));
    }
  }
  sext[1] = smax[1];

  hsize_t nlen_0 = nlen;
  if (ndims == 2) {
    if (nlen % sext[1] != 0) {
      LOG(Sev::Error, "dataset dimensions do not match");
      return {AppendResult::ERROR};
    }
    nlen_0 /= sext[1];
  }

  size_t snext = -1;
  if (not cq) {
    snext = snow[0];
  } else {
  }

  if (snext + nlen_0 > sext[0]) {
    auto t1 = CLK::now();
    // TODO
    // Make these configurable, and the default much smaller than it is right
    // now.
    uint32_t BLOCK = 22;
    if (ndims == 2) {
      size_t snow_1_ln2 = -1;
      for (size_t x = sext[1]; x != 0; x = x >> 1) {
        snow_1_ln2 += 1;
      }
      if (snow_1_ln2 >= BLOCK) {
        LOG(Sev::Error, "snow_1_ln2 >= BLOCK; {} >= {};  sext[1]: {}",
            snow_1_ln2, BLOCK, sext[1]);
        snow_1_ln2 = BLOCK - 1;
      }
      BLOCK -= snow_1_ln2;
    }
    uint32_t const MAX = BLOCK + 8;
    AT sext2;
    sext2 = sext;
    sext2[0] = sext[0];
    sext2[1] = sext[1];
    sext2[0] = (1 + (((snext + nlen_0) * 4 / 3) >> BLOCK)) << BLOCK;
    if (sext2[0] - sext[0] > (1u << MAX)) {
      sext2[0] = sext[0] + (1 << MAX);
    }
    LOG(Sev::Debug, "snext: {:12}  set_extent\n  from: {:12}  to: {:12}\n  "
                    "from: {:12}  to: {:12}",
        snext, sext.at(0), sext2.at(0), sext.at(1), sext2.at(1));

    auto t2 = CLK::now();
    if (not cq) {
      err = H5Dset_extent(id, sext2.data());
      if (err < 0) {
        LOG(Sev::Error, "H5Dset_extent failed");
        return {AppendResult::ERROR};
      }
      dsp_tgt = H5Dget_space(id);
      if (true) {
        err = H5Sget_simple_extent_dims(dsp_tgt, sext.data(), smax.data());
        if (err < 0) {
          LOG(Sev::Error, "fail H5Sget_simple_extent_dims");
          exit(1);
        }
      }
    }
    sext.at(1) = sext2.at(1);
    auto t3 = CLK::now();
    LOG(Sev::Debug, "h5d::append_data_1d set_extent: {} + {}",
        duration_cast<MS>(t2 - t1).count(), duration_cast<MS>(t3 - t2).count());
  }

  if (log_level >= 9) {
    AT sext, smax;
    LOG(Sev::Debug, "try to get the dsp dims:");
    err = H5Sget_simple_extent_dims(dsp_tgt, sext.data(), smax.data());
    if (err < 0) {
      LOG(Sev::Error, "fail H5Sget_simple_extent_dims");
      exit(1);
    }
    for (size_t i1 = 0; i1 < ndims; ++i1) {
      LOG(Sev::Debug, "H5Sget_simple_extent_dims {:20} ty: {}  {}: {:21} {:21}",
          name, static_cast<hid_t>(Type), i1, sext.at(i1), smax.at(i1));
    }
  }

  {
    AT start, count;
    start[0] = 0;
    start[1] = 0;
    count[0] = nlen_0;
    count[1] = sext[1];
    err = H5Sset_extent_simple(static_cast<hid_t>(DSPMem), ndims, count.data(),
                               count.data());
    err = H5Sselect_hyperslab(static_cast<hid_t>(DSPMem), H5S_SELECT_SET,
                              start.data(), nullptr, count.data(), nullptr);
    if (err < 0) {
      LOG(Sev::Error, "can not select mem hyperslab");
      return {AppendResult::ERROR};
    }
  }

  AT tgt_start, tgt_count;
  tgt_start[0] = snext;
  tgt_start[1] = 0;
  tgt_count[0] = nlen_0;
  tgt_count[1] = sext[1];
  if (log_level >= 9) {
    for (size_t i1 = 0; i1 < ndims; ++i1) {
      LOG(Sev::Debug, "select tgt  i1: {}  start: {}  count: {}", i1,
          tgt_start.at(i1), tgt_count.at(i1));
    }
  }
  err = H5Sselect_hyperslab(dsp_tgt, H5S_SELECT_SET, tgt_start.data(), nullptr,
                            tgt_count.data(), nullptr);
  if (err < 0) {
    LOG(Sev::Error, "can not select tgt hyperslab");
    return {AppendResult::ERROR};
  }
  auto t2 = CLK::now();
  err = H5Dwrite(id, static_cast<hid_t>(Type), static_cast<hid_t>(DSPMem),
                 dsp_tgt, pl_transfer, data);
  if (err < 0) {
    if (cq) {
    } else {
      LOG(Sev::Error, "write failed  ds_name: {}", ds_name);
    }
    if (log_level >= 7) {
      AT sext, smax;
      hid_t dsp = H5Dget_space(id);
      err = H5Sget_simple_extent_dims(dsp, sext.data(), smax.data());
      if (err < 0) {
        LOG(Sev::Error, "fail H5Sget_simple_extent_dims");
      }
      for (size_t i1 = 0; i1 < ndims; ++i1) {
        LOG(Sev::Debug, "H5Sget_simple_extent_dims {}: {:12} {:12}", i1,
            sext.at(i1), smax.at(i1));
      }
    }
    return {AppendResult::ERROR};
  }
  snow[0] = snext + nlen_0;
  snow[1] = sext[1];
  auto t3 = CLK::now();
  auto dt1 = duration_cast<MS>(t2 - t1).count();
  auto dt2 = duration_cast<MS>(t3 - t2).count();
  TotalNanosecondsSpent += dt1 + dt2;
  return {AppendResult::OK, sizeof(T) * nlen, tgt_start[0]};
}

template <typename T>
append_ret h5d::append_data_2d(T const *data, hsize_t nlen) {
  return append_data_1d(data, nlen);
}

template <typename T>
typename h5d_chunked_1d<T>::ptr
h5d_chunked_1d<T>::create(hid_t loc, string name, hsize_t chunk_bytes,
                          CollectiveQueue *cq) {
  hdf5::dataspace::Simple dsp({0}, {H5S_UNLIMITED});
  hdf5::property::DatasetCreationList dcpl;
  dcpl.chunk({std::max<hsize_t>(1, chunk_bytes / sizeof(T))});
  auto ds = h5d::create(loc, name, nat_type<T>(), dsp, dcpl, cq);
  if (!ds) {
    return nullptr;
  }
  // todo: With these changes, return ::ptr directly.  Also in 2d case.
  auto ret = new h5d_chunked_1d<T>(loc, name, move(*ds));
  return ptr(ret);
}

template <typename T>
typename h5d_chunked_1d<T>::ptr h5d_chunked_1d<T>::open(hid_t loc, string name,
                                                        CollectiveQueue *cq,
                                                        HDFIDStore *hdf_store) {
  auto ds = h5d::open(loc, name, cq, hdf_store);
  if (!ds) {
    return ptr();
  }
  return ptr(new h5d_chunked_1d<T>(loc, name, move(*ds)));
}

template <typename T>
h5d_chunked_1d<T>::h5d_chunked_1d(hid_t loc, string name, h5d ds_)
    : ds(move(ds_)) {
  if (ds.id < 0) {
    LOG(Sev::Critical, "not a dataset");
  } else {
    if (H5Iis_valid(ds.id) != 1) {
      LOG(Sev::Critical, "invalid ds.id: {}", ds.id);
    } else {
      if (H5Iget_type(ds.id) != H5I_DATASET) {
        LOG(Sev::Critical, "not H5I_DATASET ds.id: {}", ds.id);
      } else {
        auto dsp = H5Dget_space(ds.id);
        if (dsp < 0) {
          LOG(Sev::Critical, "failed H5Dget_space");
        } else {
          auto ndims = H5Sget_simple_extent_ndims(dsp);
          if (ndims != 1) {
            LOG(Sev::Critical, "wrong dimension ds.id: {}  dsp: {}  ndims: {}",
                ds.id, dsp, ndims);
          } else {
            std::vector<hsize_t> snow(1), smax(1);
            H5Sget_simple_extent_dims(dsp, snow.data(), smax.data());
            dsp_wr = hdf5::dataspace::Simple(snow, smax);
          }
        }
      }
    }
  }
}

template <typename T>
h5d_chunked_1d<T>::h5d_chunked_1d(h5d_chunked_1d &&x)
    : ds(move(x.ds)), dsp_wr(move(x.dsp_wr)) {}

template <typename T> h5d_chunked_1d<T>::~h5d_chunked_1d() {
  LOG(Sev::Debug,
      "~h5d_chunked_1d  count_append_calls: {}, count_append_bytes: {}, "
      "count_buffer_copy_calls: {}, count_buffer_copy_bytes: {}",
      count_append_calls, count_append_bytes, count_buffer_copy_calls,
      count_buffer_copy_bytes);
  flush_buf();
}

template <typename T> void swap(h5d_chunked_1d<T> &x, h5d_chunked_1d<T> &y) {
  swap(x.ds, y.ds);
  swap(x.dsp_wr, y.dsp_wr);
  swap(x.i0, y.i0);
  swap(x.buf_packet_max, y.buf_packet_max);
  swap(x.buf_size, y.buf_size);
  swap(x.buf, y.buf);
  swap(x.buf_n, y.buf_n);
}

template <typename T>
void h5d_chunked_1d<T>::buffer_init(size_t buf_size, size_t buf_packet_max) {
  this->buf_size = buf_size;
  this->buf_packet_max = buf_packet_max;
  buf.resize(buf_size);
}

template <typename T>
append_ret h5d_chunked_1d<T>::append_data_1d(T const *data, hsize_t nlen) {
  auto nbytes = nlen * sizeof(T);
  bool do_buf = nbytes <= buf_packet_max;
  auto buffer_append = [this, &nbytes](T const *data) {
    if (buf_n + nbytes > buf_size) {
      LOG(Sev::Error, "fail buffer");
      exit(1);
    }
    auto p1 = (char *)data;
    auto p2 = buf.data() + buf_n;
    for (size_t i1 = 0; i1 < nbytes; ++i1) {
      p2[i1] = p1[i1];
    }
    buf_n += nbytes;
    count_buffer_copy_calls += 1;
    count_buffer_copy_bytes += nbytes;
  };
  if (do_buf) {
    buffer_append(data);
  }
  // Flush the buffer if there is a chance that it will be full on the next
  // iteration.
  if (buf_n + buf_packet_max > buf_size || (!do_buf && buf_n > 0)) {
    auto res = flush_buf();
    if (res == AppendResult::ERROR) {
      return {res};
    } else if (res != AppendResult::OK) {
      LOG(Sev::Error, "unhandled error");
      exit(1);
    }
  }
  if (!do_buf) {
    auto res = ds.append_data_1d(data, nlen);
    if (res.status == AppendResult::ERROR) {
      return res;
    }
  }
  append_ret ret;
  ret.status = AppendResult::OK;
  ret.ix0 = i0;
  ret.written_bytes = nbytes;
  i0 += nlen;
  count_append_calls += 1;
  count_append_bytes += nbytes;
  return ret;
}

template <typename T> AppendResult h5d_chunked_1d<T>::flush_buf() {
  auto wr = ds.append_data_1d((T *)buf.data(), buf_n / sizeof(T));
  if (wr.status != AppendResult::OK) {
    LOG(Sev::Debug, "FLUSH NOT OK");
    return wr.status;
  }
  buf_n = 0;
  return AppendResult::OK;
}

template <typename T>
typename h5d_chunked_2d<T>::ptr
h5d_chunked_2d<T>::create(hid_t loc, string name, hsize_t ncols,
                          hsize_t chunk_bytes, CollectiveQueue *cq) {
  hdf5::dataspace::Simple dsp({0, ncols}, {H5S_UNLIMITED, ncols});
  hdf5::property::DatasetCreationList dcpl;
  dcpl.chunk(
      {std::max<hsize_t>(1, chunk_bytes / ncols / H5Tget_size(nat_type<T>())),
       ncols});
  auto ds = h5d::create(loc, name, nat_type<T>(), dsp, dcpl, cq);
  if (!ds) {
    return nullptr;
  }
  auto ret = new h5d_chunked_2d<T>(loc, name, move(*ds), ncols);
  return ptr(ret);
}

template <typename T>
typename h5d_chunked_2d<T>::ptr
h5d_chunked_2d<T>::open(hid_t loc, string name, hsize_t ncols,
                        CollectiveQueue *cq, HDFIDStore *hdf_store) {
  auto ds = h5d::open(loc, name, cq, hdf_store);
  if (!ds) {
    return ptr();
  }
  return ptr(new h5d_chunked_2d<T>(loc, name, move(*ds), ncols));
}

template <typename T>
h5d_chunked_2d<T>::h5d_chunked_2d(hid_t loc, string name, h5d ds_,
                                  hsize_t ncols)
    : ds(move(ds_)), ncols(ncols) {
  if (ds.id < 0) {
    LOG(Sev::Critical, "not a dataset");
  } else {
    if (H5Iis_valid(ds.id) != 1) {
      LOG(Sev::Critical, "invalid ds.id: {}", ds.id);
    } else {
      if (H5Iget_type(ds.id) != H5I_DATASET) {
        LOG(Sev::Critical, "not H5I_DATASET ds.id: {}", ds.id);
      } else {
        auto dsp = H5Dget_space(ds.id);
        if (dsp < 0) {
          LOG(Sev::Critical, "failed H5Dget_space");
        } else {
          auto ndims = H5Sget_simple_extent_ndims(dsp);
          if (ndims != 2) {
            LOG(Sev::Critical, "wrong dimension ds.id: {}  dsp: {}  ndims: {}",
                ds.id, dsp, ndims);
          } else {
            std::vector<hsize_t> snow(2), smax(2);
            H5Sget_simple_extent_dims(dsp, snow.data(), smax.data());
            dsp_wr = hdf5::dataspace::Simple(snow, smax);
          }
        }
      }
    }
  }
}

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
void h5d_chunked_2d<T>::buffer_init(size_t buf_size, size_t buf_packet_max) {
  this->buf_size = buf_size;
  this->buf_packet_max = buf_packet_max;
  buf.resize(buf_size);
}

template <typename T>
append_ret h5d_chunked_2d<T>::append_data_2d(T const *data, hsize_t nlen) {
  auto nbytes = nlen * sizeof(T);
  bool do_buf = nbytes <= buf_packet_max;
  auto buffer_append = [this, &nbytes](T const *data) {
    if (buf_n + nbytes > buf_size) {
      LOG(Sev::Error, "fail buffer");
      exit(1);
    }
    auto p1 = (char *)data;
    auto p2 = buf.data() + buf_n;
    for (size_t i1 = 0; i1 < nbytes; ++i1) {
      p2[i1] = p1[i1];
    }
    buf_n += nbytes;
    count_buffer_copy_calls += 1;
    count_buffer_copy_bytes += nbytes;
  };
  if (do_buf) {
    buffer_append(data);
  }
  // Flush the buffer if there is a chance that it will be full on the next
  // iteration.
  if (buf_n + buf_packet_max > buf_size || (!do_buf && buf_n > 0)) {
    auto res = flush_buf();
    if (res == AppendResult::ERROR) {
      return {res};
    } else if (res != AppendResult::OK) {
      LOG(Sev::Error, "unhandled error");
      exit(1);
    }
  }
  if (!do_buf) {
    auto res = ds.append_data_1d(data, nlen);
    if (res.status == AppendResult::ERROR) {
      return res;
    }
  }
  append_ret ret;
  ret.status = AppendResult::OK;
  ret.ix0 = i0;
  ret.written_bytes = nbytes;
  i0 += nlen;
  count_append_calls += 1;
  count_append_bytes += nbytes;
  return ret;
}

template <typename T> AppendResult h5d_chunked_2d<T>::flush_buf() {
  auto wr = ds.append_data_2d((T *)buf.data(), buf_n / sizeof(T));
  if (wr.status != AppendResult::OK) {
    return wr.status;
  }
  buf_n = 0;
  return AppendResult::OK;
}

// clang-format off

template append_ret h5d::append_data_1d(uint8_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(uint16_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(uint32_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(uint64_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(int8_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(int16_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(int32_t const *data, hsize_t nlen);
template append_ret h5d::append_data_1d(int64_t const *data, hsize_t nlen);

template h5d_chunked_1d<uint8_t>::ptr
h5d_chunked_1d<uint8_t>::create(hid_t loc, string name, hsize_t chunk_bytes,
                                CollectiveQueue *cq);
template h5d_chunked_1d<uint16_t>::ptr
h5d_chunked_1d<uint16_t>::create(hid_t loc, string name, hsize_t chunk_bytes,
                                 CollectiveQueue *cq);
template h5d_chunked_1d<uint32_t>::ptr
h5d_chunked_1d<uint32_t>::create(hid_t loc, string name, hsize_t chunk_bytes,
                                 CollectiveQueue *cq);
template h5d_chunked_1d<uint64_t>::ptr
h5d_chunked_1d<uint64_t>::create(hid_t loc, string name, hsize_t chunk_bytes,
                                 CollectiveQueue *cq);
template h5d_chunked_1d<int8_t>::ptr
h5d_chunked_1d<int8_t>::create(hid_t loc, string name, hsize_t chunk_bytes,
                               CollectiveQueue *cq);
template h5d_chunked_1d<int16_t>::ptr
h5d_chunked_1d<int16_t>::create(hid_t loc, string name, hsize_t chunk_bytes,
                                CollectiveQueue *cq);
template h5d_chunked_1d<int32_t>::ptr
h5d_chunked_1d<int32_t>::create(hid_t loc, string name, hsize_t chunk_bytes,
                                CollectiveQueue *cq);
template h5d_chunked_1d<int64_t>::ptr
h5d_chunked_1d<int64_t>::create(hid_t loc, string name, hsize_t chunk_bytes,
                                CollectiveQueue *cq);
template h5d_chunked_1d<float>::ptr
h5d_chunked_1d<float>::create(hid_t loc, string name, hsize_t chunk_bytes,
                              CollectiveQueue *cq);
template h5d_chunked_1d<double>::ptr
h5d_chunked_1d<double>::create(hid_t loc, string name, hsize_t chunk_bytes,
                               CollectiveQueue *cq);

template h5d_chunked_1d< uint8_t>::ptr h5d_chunked_1d< uint8_t>::open(hid_t loc, string name, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_1d<uint16_t>::ptr h5d_chunked_1d<uint16_t>::open(hid_t loc, string name, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_1d<uint32_t>::ptr h5d_chunked_1d<uint32_t>::open(hid_t loc, string name, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_1d<uint64_t>::ptr h5d_chunked_1d<uint64_t>::open(hid_t loc, string name, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_1d<  int8_t>::ptr h5d_chunked_1d<  int8_t>::open(hid_t loc, string name, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_1d< int16_t>::ptr h5d_chunked_1d< int16_t>::open(hid_t loc, string name, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_1d< int32_t>::ptr h5d_chunked_1d< int32_t>::open(hid_t loc, string name, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_1d< int64_t>::ptr h5d_chunked_1d< int64_t>::open(hid_t loc, string name, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_1d<   float>::ptr h5d_chunked_1d<   float>::open(hid_t loc, string name, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_1d<  double>::ptr h5d_chunked_1d<  double>::open(hid_t loc, string name, CollectiveQueue *cq, HDFIDStore *hdf_store);

template h5d_chunked_1d<uint8_t>::h5d_chunked_1d(hid_t loc, string name,
                                                 h5d ds);
template h5d_chunked_1d<uint16_t>::h5d_chunked_1d(hid_t loc, string name,
                                                  h5d ds);
template h5d_chunked_1d<uint32_t>::h5d_chunked_1d(hid_t loc, string name,
                                                  h5d ds);
template h5d_chunked_1d<uint64_t>::h5d_chunked_1d(hid_t loc, string name,
                                                  h5d ds);
template h5d_chunked_1d<int8_t>::h5d_chunked_1d(hid_t loc, string name, h5d ds);
template h5d_chunked_1d<int16_t>::h5d_chunked_1d(hid_t loc, string name,
                                                 h5d ds);
template h5d_chunked_1d<int32_t>::h5d_chunked_1d(hid_t loc, string name,
                                                 h5d ds);
template h5d_chunked_1d<int64_t>::h5d_chunked_1d(hid_t loc, string name,
                                                 h5d ds);
template h5d_chunked_1d<float>::h5d_chunked_1d(hid_t loc, string name, h5d ds);
template h5d_chunked_1d<double>::h5d_chunked_1d(hid_t loc, string name, h5d ds);

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

template void h5d_chunked_1d< uint8_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_1d<uint16_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_1d<uint32_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_1d<uint64_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_1d<  int8_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_1d< int16_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_1d< int32_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_1d< int64_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_1d<   float>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_1d<  double>::buffer_init(size_t buf_size, size_t buf_packet_max);

template void h5d_chunked_2d< uint8_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_2d<uint16_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_2d<uint32_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_2d<uint64_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_2d<  int8_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_2d< int16_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_2d< int32_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_2d< int64_t>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_2d<   float>::buffer_init(size_t buf_size, size_t buf_packet_max);
template void h5d_chunked_2d<  double>::buffer_init(size_t buf_size, size_t buf_packet_max);

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
                                hsize_t chunk_bytes, CollectiveQueue *cq);
template h5d_chunked_2d<uint16_t>::ptr
h5d_chunked_2d<uint16_t>::create(hid_t loc, string name, hsize_t ncols,
                                 hsize_t chunk_bytes, CollectiveQueue *cq);
template h5d_chunked_2d<uint32_t>::ptr
h5d_chunked_2d<uint32_t>::create(hid_t loc, string name, hsize_t ncols,
                                 hsize_t chunk_bytes, CollectiveQueue *cq);
template h5d_chunked_2d<uint64_t>::ptr
h5d_chunked_2d<uint64_t>::create(hid_t loc, string name, hsize_t ncols,
                                 hsize_t chunk_bytes, CollectiveQueue *cq);
template h5d_chunked_2d<int8_t>::ptr
h5d_chunked_2d<int8_t>::create(hid_t loc, string name, hsize_t ncols,
                               hsize_t chunk_bytes, CollectiveQueue *cq);
template h5d_chunked_2d<int16_t>::ptr
h5d_chunked_2d<int16_t>::create(hid_t loc, string name, hsize_t ncols,
                                hsize_t chunk_bytes, CollectiveQueue *cq);
template h5d_chunked_2d<int32_t>::ptr
h5d_chunked_2d<int32_t>::create(hid_t loc, string name, hsize_t ncols,
                                hsize_t chunk_bytes, CollectiveQueue *cq);
template h5d_chunked_2d<int64_t>::ptr
h5d_chunked_2d<int64_t>::create(hid_t loc, string name, hsize_t ncols,
                                hsize_t chunk_bytes, CollectiveQueue *cq);
template h5d_chunked_2d<float>::ptr
h5d_chunked_2d<float>::create(hid_t loc, string name, hsize_t ncols,
                              hsize_t chunk_bytes, CollectiveQueue *cq);
template h5d_chunked_2d<double>::ptr
h5d_chunked_2d<double>::create(hid_t loc, string name, hsize_t ncols,
                               hsize_t chunk_bytes, CollectiveQueue *cq);


template h5d_chunked_2d< uint8_t>::ptr h5d_chunked_2d< uint8_t>::open(hid_t loc, string name, hsize_t ncols, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_2d<uint16_t>::ptr h5d_chunked_2d<uint16_t>::open(hid_t loc, string name, hsize_t ncols, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_2d<uint32_t>::ptr h5d_chunked_2d<uint32_t>::open(hid_t loc, string name, hsize_t ncols, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_2d<uint64_t>::ptr h5d_chunked_2d<uint64_t>::open(hid_t loc, string name, hsize_t ncols, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_2d<  int8_t>::ptr h5d_chunked_2d<  int8_t>::open(hid_t loc, string name, hsize_t ncols, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_2d< int16_t>::ptr h5d_chunked_2d< int16_t>::open(hid_t loc, string name, hsize_t ncols, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_2d< int32_t>::ptr h5d_chunked_2d< int32_t>::open(hid_t loc, string name, hsize_t ncols, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_2d< int64_t>::ptr h5d_chunked_2d< int64_t>::open(hid_t loc, string name, hsize_t ncols, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_2d<   float>::ptr h5d_chunked_2d<   float>::open(hid_t loc, string name, hsize_t ncols, CollectiveQueue *cq, HDFIDStore *hdf_store);
template h5d_chunked_2d<  double>::ptr h5d_chunked_2d<  double>::open(hid_t loc, string name, hsize_t ncols, CollectiveQueue *cq, HDFIDStore *hdf_store);


template h5d_chunked_2d< uint8_t>::h5d_chunked_2d(hid_t loc, string name, h5d ds, hsize_t ncols);
template h5d_chunked_2d<uint16_t>::h5d_chunked_2d(hid_t loc, string name, h5d ds, hsize_t ncols);
template h5d_chunked_2d<uint32_t>::h5d_chunked_2d(hid_t loc, string name, h5d ds, hsize_t ncols);
template h5d_chunked_2d<uint64_t>::h5d_chunked_2d(hid_t loc, string name, h5d ds, hsize_t ncols);
template h5d_chunked_2d<  int8_t>::h5d_chunked_2d(hid_t loc, string name, h5d ds, hsize_t ncols);
template h5d_chunked_2d< int16_t>::h5d_chunked_2d(hid_t loc, string name, h5d ds, hsize_t ncols);
template h5d_chunked_2d< int32_t>::h5d_chunked_2d(hid_t loc, string name, h5d ds, hsize_t ncols);
template h5d_chunked_2d< int64_t>::h5d_chunked_2d(hid_t loc, string name, h5d ds, hsize_t ncols);
template h5d_chunked_2d<   float>::h5d_chunked_2d(hid_t loc, string name, h5d ds, hsize_t ncols);
template h5d_chunked_2d<  double>::h5d_chunked_2d(hid_t loc, string name, h5d ds, hsize_t ncols);


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

// clang-format on

} // namespace h5
