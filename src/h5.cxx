#include "h5.h"
#include "logger.h"
#include <chrono>

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
                     h5p::dataset_create dcpl, CollectiveQueue *cq) {
  // Creation is done in single process mode.
  // Can do set_extent here.
  LOG(3, "h5d::create");
  auto ret = ptr(new h5d);
  auto &o = *ret;
  herr_t err = 0;
  o.type = type;
  o.name = name;
  err = H5Pset_fill_value(dcpl.id, type, nullptr);
  if (err < 0) {
    LOG(7, "failed H5Pset_fill_value");
  }
  o.id = H5Dcreate1(loc, name.c_str(), type, dsp.id, dcpl.id);
  if (o.id < 0) {
    LOG(3, "H5Dcreate1 failed");
    ret.reset();
    return ret;
  }

  {
    char buf[512];
    auto bufn = H5Iget_name(o.id, buf, 512);
    buf[bufn] = '\0';
    cq->register_datasetname(buf);
    // Do not keep the cq for later.
    // In the initial setup, we do not want to use the cq.
  }

  if (false) {
    // TODO
    LOG(3, "SET INITAL EXTENT, REMOVE LATER");
    hid_t dsp_tgt = H5Dget_space(o.id);
    hid_t id = o.id;
    herr_t err = 0;
    std::array<hsize_t, 2> sext;
    std::array<hsize_t, 2> smax;
    H5Sget_simple_extent_dims(dsp_tgt, sext.data(), smax.data());
    sext[0] += 32;
    err = H5Dset_extent(id, sext.data());
    if (err < 0) {
      LOG(3, "fail H5Dset_extent");
      exit(1);
    }
  }

  o.dsp_tgt = H5Dget_space(o.id);

  o.ndims = H5Sget_simple_extent_ndims(o.dsp_tgt);
  o.snow = {{0, 0}};
  H5Sget_simple_extent_dims(o.dsp_tgt, o.sext.data(), o.smax.data());
  if (log_level >= 9) {
    for (size_t i1 = 0; i1 < o.ndims; ++i1) {
      LOG(9, "H5Sget_simple_extent_dims {:20} ty: {}  {}: {:21} {:21}", o.name,
          o.type, i1, o.sext.at(i1), o.smax.at(i1));
    }
  }
  o.mem_max = {{100000000, 100000000}};
  o.mem_now = {{0, 0}};
  o.dsp_mem = H5Screate_simple(o.ndims, o.mem_max.data(), nullptr);
  if (o.dsp_mem < 0) {
    LOG(3, "H5Screate_simple dsp_mem failed");
  }
  o.pl_transfer = H5Pcreate(H5P_DATASET_XFER);
  err = H5Pset_edc_check(o.pl_transfer, H5Z_DISABLE_EDC);
  if (err < 0) {
    LOG(7, "failed H5Pset_edc_check");
  }
  if (false) {
    err = H5Pset_dxpl_mpio(o.pl_transfer, H5FD_MPIO_COLLECTIVE);
    if (err < 0) {
      LOG(7, "failed H5Pset_dxpl_mpio");
    }
  }
  return ret;
}

h5d::ptr h5d::open(hid_t loc, string name, CollectiveQueue *cq,
                   HDFIDStore *hdf_store) {
  if (cq) {
    herr_t err = 0;
    char buf[512];
    {
      auto bufn = H5Iget_name(loc, buf, 512);
      buf[bufn] = '\0';
    }
    std::string full_path(buf);
    full_path += "/";
    full_path += name;
    LOG(3, "h5d::open collective [{}]", full_path);
    cq->push(*hdf_store, 0, CollectiveCommand::H5Dopen2(full_path.c_str()));
    cq->execute_for(*hdf_store, 0);

    auto ret = ptr(new h5d);
    auto &o = *ret;
    o.cq = cq;
    o.hdf_store = hdf_store;
    o.name = name;
    o.id = hdf_store->datasetname_to_ds_id[full_path];
    LOG(3, "that created  rank: {}  id: {}  for path: {}", hdf_store->mpi_rank,
        o.id, full_path);
    o.type = H5Dget_type(o.id);
    o.dsp_tgt = H5Dget_space(o.id);
    o.ndims = H5Sget_simple_extent_ndims(o.dsp_tgt);
    o.snow = {{0, 0}};
    H5Sget_simple_extent_dims(o.dsp_tgt, o.sext.data(), o.smax.data());
    if (log_level >= 9) {
      for (size_t i1 = 0; i1 < o.ndims; ++i1) {
        LOG(9, "H5Sget_simple_extent_dims {:20} ty: {}  {}: {:21} {:21}",
            o.name, o.type, i1, o.sext.at(i1), o.smax.at(i1));
      }
    }
    o.mem_max = {{100000000, 100000000}};
    o.mem_now = {{0, 0}};
    o.dsp_mem = H5Screate_simple(o.ndims, o.mem_max.data(), nullptr);
    if (o.dsp_mem < 0) {
      LOG(3, "H5Screate_simple dsp_mem failed");
    }
    o.pl_transfer = H5Pcreate(H5P_DATASET_XFER);
    err = H5Pset_edc_check(o.pl_transfer, H5Z_DISABLE_EDC);
    if (err < 0) {
      LOG(7, "failed H5Pset_edc_check");
    }
    if (false) {
      err = H5Pset_dxpl_mpio(o.pl_transfer, H5FD_MPIO_COLLECTIVE);
      if (err < 0) {
        LOG(7, "failed H5Pset_dxpl_mpio");
      }
    }
    return ret;
  } else {
    LOG(3, "h5d::open classic");
    auto ret = ptr(new h5d);
    auto &o = *ret;
    herr_t err = 0;
    o.name = name;
    o.id = H5Dopen2(loc, name.c_str(), H5P_DEFAULT);
    if (o.id < 0) {
      LOG(3, "H5Dopen2 failed");
      ret.reset();
      return ret;
    }
    o.type = H5Dget_type(o.id);
    o.dsp_tgt = H5Dget_space(o.id);
    o.ndims = H5Sget_simple_extent_ndims(o.dsp_tgt);
    o.snow = {{0, 0}};
    H5Sget_simple_extent_dims(o.dsp_tgt, o.sext.data(), o.smax.data());
    if (log_level >= 9) {
      for (size_t i1 = 0; i1 < o.ndims; ++i1) {
        LOG(9, "H5Sget_simple_extent_dims {:20} ty: {}  {}: {:21} {:21}",
            o.name, o.type, i1, o.sext.at(i1), o.smax.at(i1));
      }
    }
    o.mem_max = {{100000000, 100000000}};
    o.mem_now = {{0, 0}};
    o.dsp_mem = H5Screate_simple(o.ndims, o.mem_max.data(), nullptr);
    if (o.dsp_mem < 0) {
      LOG(3, "H5Screate_simple dsp_mem failed");
    }
    o.pl_transfer = H5Pcreate(H5P_DATASET_XFER);
    err = H5Pset_edc_check(o.pl_transfer, H5Z_DISABLE_EDC);
    if (err < 0) {
      LOG(7, "failed H5Pset_edc_check");
    }
    if (false) {
      err = H5Pset_dxpl_mpio(o.pl_transfer, H5FD_MPIO_COLLECTIVE);
      if (err < 0) {
        LOG(7, "failed H5Pset_dxpl_mpio");
      }
    }
    return ret;
  }
}

h5d::h5d(h5d &&x) { swap(*this, x); }

h5d::~h5d() {
  if (id != -1) {
    LOG(9, "~h5d ds");
    herr_t err = 0;
    char ds_name[512];
    auto &buf = ds_name;
    auto bufn = H5Iget_name(id, buf, 512);
    H5O_info_t oi;
    H5Oget_info(id, &oi);
    if (not cq) {
      err = H5Dset_extent(id, snow.data());
      if (err < 0) {
        LOG(3, "H5Dset_extent failed");
      }
      err = H5Dclose(id);
      if (err < 0) {
        LOG(3, "Could not close dataset: {}");
        LOG(7, "error closing dataset  {}  {:.{}}  H5Iget_ref: {}  "
               "H5O_info_t.rc: {}",
            id, buf, bufn, H5Iget_ref(id), oi.rc);
      }
      id = -1;
    } else {
      LOG(9, "~h5d ds  cqid: {}", hdf_store->cqid);
      size_t CQSNOWIX = -1;
      lookup_cqsnowix(ds_name, CQSNOWIX);
      snow[0] = cq->snow[CQSNOWIX].load();
      // TODO
      // Trim the final size, needs some more work.
      cq->push(*hdf_store, 1, CollectiveCommand::set_extent(
                                  ds_name, snow.size(), snow.data()));
      cq->push(*hdf_store, 2, CollectiveCommand::H5Dclose(ds_name));
      id = -1;
    }
  }
  if (dsp_mem != -1) {
    H5Sclose(dsp_mem);
    dsp_mem = -1;
  }
  if (pl_transfer != -1) {
    H5Pclose(pl_transfer);
    pl_transfer = -1;
  }
  if (type != -1) {
    // TODO
    // H5Tclose(type);
    type = -1;
  }
}

h5d::h5d() {}

void swap(h5d &x, h5d &y) {
  using std::swap;
  swap(x.id, y.id);
  swap(x.name, y.name);
  swap(x.type, y.type);
  swap(x.pl_transfer, y.pl_transfer);
  swap(x.ndims, y.ndims);
  swap(x.dsp_mem, y.dsp_mem);
  swap(x.dsp_tgt, y.dsp_tgt);
  swap(x.snow, y.snow);
  swap(x.smax, y.smax);
  swap(x.sext, y.sext);
  swap(x.mem_max, y.mem_max);
  swap(x.mem_now, y.mem_now);
  swap(x.cq, y.cq);
  swap(x.hdf_store, y.hdf_store);
  swap(x.mpi_rank, y.mpi_rank);
}

void h5d::lookup_cqsnowix(char const *ds_name, size_t &cqsnowix) {
  LOG(9, "using cq: {}", (void *)cq);
  cqsnowix = cq->find_snowix_for_datasetname(ds_name);
}

template <typename T>
append_ret h5d::append_data_1d(T const *data, hsize_t nlen) {
  using namespace std::chrono;
  using CLK = steady_clock;
  using MS = milliseconds;
  auto t1 = CLK::now();
  LOG(9, "append_data_1d");
  if (log_level >= 9) {
    array<char, 64> buf1;
    auto n1 = H5Iget_name(id, buf1.data(), buf1.size());
    if (n1 > 0) {
      LOG(9, "append_data_1d {} for dataset {:.{}}", nlen, buf1.data(), n1);
    }
  }
  using A1 = array<hsize_t, 1>;
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

  size_t CQSNOWIX = -1;
  if (cq) {
    lookup_cqsnowix(ds_name, CQSNOWIX);
    dsp_tgt = hdf_store->datasetname_to_dsp_id[ds_name];
    // Not necessary, just for testing:
    if (false) {
      LOG(9, "try to get the dsp dims:");
      err = H5Sget_simple_extent_dims(dsp_tgt, sext.data(), smax.data());
      if (err < 0) {
        LOG(3, "fail H5Sget_simple_extent_dims");
        exit(1);
      }
    }
  } else {
    LOG(9, "DO NOT LOOKUP CQSNOWIX");
  }

  size_t snext = -1;
  if (not cq) {
    snext = snow[0];
  } else {
    LOG(9, "CAS allocate...");
    while (true) {
      snext = cq->snow[CQSNOWIX].load();
      if (cq->snow[CQSNOWIX].compare_exchange_weak(snext, snext + nlen)) {
        break;
      }
    }
    LOG(9, "CAS allocated: {}", snext);
  }

  if (snext + nlen > sext[0]) {
    auto t1 = CLK::now();
    // TODO
    // Make these configurable, and the default much smaller than it is right
    // now.
    uint32_t const BLOCK = 22;
    uint32_t const MAX = BLOCK + 8;
    std::array<hsize_t, 2> sext2;
    sext2[0] = sext[0];
    sext2[1] = sext[1];
    sext2[0] = (1 + (((snext + nlen) * 4 / 3) >> BLOCK)) << BLOCK;
    if (sext2[0] - sext[0] > (1 << MAX)) {
      sext2[0] = sext[0] + (1 << MAX);
    }
    LOG(7, "snext: {:12}  set_extent from: {:12}  to: {:12}", snext, sext[0],
        sext2[0]);

    auto t2 = CLK::now();
    if (not cq) {
      err = H5Dset_extent(id, sext2.data());
      if (err < 0) {
        LOG(3, "H5Dset_extent failed");
        return {AppendResult::ERROR};
      }
      dsp_tgt = H5Dget_space(id);
      if (true) {
        // LOG(9, "try to get the dsp dims:");
        err = H5Sget_simple_extent_dims(dsp_tgt, sext.data(), smax.data());
        if (err < 0) {
          LOG(3, "fail H5Sget_simple_extent_dims");
          exit(1);
        }
      }
    } else {
      // H5O_info_t oi;
      // H5Oget_info(id, &oi);
      cq->push(*hdf_store, 0, CollectiveCommand::set_extent(
                                  ds_name, sext2.size(), sext2.data()));
      cq->execute_for(*hdf_store, 0);
      dsp_tgt = hdf_store->datasetname_to_dsp_id[ds_name];
      if (true) {
        // LOG(9, "try to get the dsp dims:");
        err = H5Sget_simple_extent_dims(dsp_tgt, sext.data(), smax.data());
        if (err < 0) {
          LOG(3, "fail H5Sget_simple_extent_dims");
          exit(1);
        }
      }
    }
    auto t3 = CLK::now();
    LOG(7, "h5d::append_data_1d set_extent: {} + {}",
        duration_cast<MS>(t2 - t1).count(), duration_cast<MS>(t3 - t2).count());
  }

  if (log_level >= 9) {
    A1 sext, smax;
    LOG(9, "try to get the dsp dims:");
    err = H5Sget_simple_extent_dims(dsp_tgt, sext.data(), smax.data());
    if (err < 0) {
      LOG(3, "fail H5Sget_simple_extent_dims");
      exit(1);
    }
    for (size_t i1 = 0; i1 < ndims; ++i1) {
      LOG(9, "H5Sget_simple_extent_dims {:20} ty: {}  {}: {:21} {:21}", name,
          type, i1, sext.at(i1), smax.at(i1));
    }
  }

  {
    A1 start{{0}};
    A1 count{{nlen}};
    err = H5Sselect_hyperslab(dsp_mem, H5S_SELECT_SET, start.data(), nullptr,
                              count.data(), nullptr);
    if (err < 0) {
      LOG(3, "can not select mem hyperslab");
      return {AppendResult::ERROR};
    }
  }

  A1 tgt_start{{snext}};
  A1 tgt_count{{nlen}};
  if (log_level >= 9) {
    for (size_t i1 = 0; i1 < ndims; ++i1) {
      LOG(9, "select tgt  i1: {}  start: {}  count: {}", i1, tgt_start[0],
          tgt_count[0]);
    }
  }
  err = H5Sselect_hyperslab(dsp_tgt, H5S_SELECT_SET, tgt_start.data(), nullptr,
                            tgt_count.data(), nullptr);
  if (err < 0) {
    LOG(3, "can not select tgt hyperslab");
    return {AppendResult::ERROR};
  }
  auto t2 = CLK::now();
  err = H5Dwrite(id, type, dsp_mem, dsp_tgt, pl_transfer, data);
  if (err < 0) {
    LOG(3, "write failed");
    if (log_level >= 7) {
      std::array<hsize_t, 4> sext;
      std::array<hsize_t, 4> smax;
      hid_t dsp = H5Dget_space(id);
      err = H5Sget_simple_extent_dims(dsp, sext.data(), smax.data());
      if (err < 0) {
        LOG(3, "fail H5Sget_simple_extent_dims");
        exit(1);
      }
      for (size_t i1 = 0; i1 < 1; ++i1) {
        LOG(7, "H5Sget_simple_extent_dims {}: {:12} {:12}", i1, sext.at(i1),
            smax.at(i1));
      }
    }
    exit(1);
    return {AppendResult::ERROR};
  }
  snow[0] = snext + nlen;
  auto t3 = CLK::now();
  auto dt1 = duration_cast<MS>(t2 - t1).count();
  auto dt2 = duration_cast<MS>(t3 - t2).count();
  // TODO gather stats
  // LOG(9, "append_data_1d DONE  {} ms + {} ms", dt1, dt2);
  return {AppendResult::OK, sizeof(T) * nlen, tgt_start[0]};
}

template <typename T>
append_ret h5d::append_data_2d(T const *data, hsize_t nlen) {
  LOG(3, "append_data_2d");
  LOG(3, "NO LONGER SUPPORTED");
  // TODO
  // adapt to 1d case
  exit(1);
  if (log_level >= 9) {
    array<char, 64> buf1;
    auto n1 = H5Iget_name(id, buf1.data(), buf1.size());
    if (n1 > 0) {
      LOG(9, "append_data_2d {} for dataset {:.{}}", nlen, buf1.data(), n1);
    }
  }
  using A1 = array<hsize_t, 2>;
  herr_t err;
  hsize_t ncols = snow[1];
  if (nlen % ncols != 0) {
    LOG(3, "dataset dimensions do not match");
    return {AppendResult::ERROR};
  }
  hsize_t nrows = nlen / ncols;
  snow[0] += nrows;
  if (snow[0] > sext[0]) {
    size_t const BLOCK = 1 * 1024 * 1024;
    sext[0] = (snow[0] * 3 / 2 / BLOCK + 1) * BLOCK;

    char buf[512];
    auto bufn = H5Iget_name(id, buf, 512);
    buf[bufn] = '\0';
    H5O_info_t oi;
    H5Oget_info(id, &oi);
    if (not cq) {
      err = H5Dset_extent(id, sext.data());
      if (err < 0) {
        LOG(3, "H5Dset_extent failed");
        return {AppendResult::ERROR};
      }
    } else if (mpi_rank == 1) {
      LOG(3, "NOT IMPLEMENTED");
      exit(1);
    }

    err = H5Dset_extent(id, snow.data());
    if (err < 0) {
      LOG(3, "can not extend dataset");
      return {AppendResult::ERROR};
    }

    dsp_tgt = H5Dget_space(id);
  }
  {
    A1 start{{0, 0}};
    A1 count{{nrows, ncols}};
    err = H5Sselect_hyperslab(dsp_mem, H5S_SELECT_SET, start.data(), nullptr,
                              count.data(), nullptr);
    if (err < 0) {
      LOG(3, "can not select mem hyperslab");
      return {AppendResult::ERROR};
    }
  }
  A1 tgt_start{{snow[0] - nrows, 0}};
  A1 tgt_count{{nrows, ncols}};
  err = H5Sselect_hyperslab(dsp_tgt, H5S_SELECT_SET, tgt_start.data(), nullptr,
                            tgt_count.data(), nullptr);
  if (err < 0) {
    LOG(3, "can not select tgt hyperslab");
    return {AppendResult::ERROR};
  }
  err = H5Dwrite(id, type, dsp_mem, dsp_tgt, pl_transfer, data);
  if (err < 0) {
    LOG(3, "writing failed");
    return {AppendResult::ERROR};
  }
  return {AppendResult::OK, sizeof(T) * nlen, tgt_start[0]};
}

template <typename T>
typename h5d_chunked_1d<T>::ptr
h5d_chunked_1d<T>::create(hid_t loc, string name, hsize_t chunk_bytes,
                          CollectiveQueue *cq) {
  auto dsp = h5s::simple_unlim<1>({{0}});
  if (!dsp) {
    return nullptr;
  }
  auto dcpl = h5p::dataset_create::chunked1(nat_type<T>(), chunk_bytes);
  if (!dcpl) {
    return nullptr;
  }
  auto ds = h5d::create(loc, name, nat_type<T>(), move(*dsp), move(*dcpl), cq);
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
  /*
  TODO
  - Fetch the dsp after open
  - Something to do with dcpl??

  auto dsp = h5s::simple_unlim<1>({{0}});
  if (!dsp) {
    return nullptr;
  }
  auto dcpl = h5p::dataset_open::chunked1(nat_type<T>(), chunk_bytes);
  if (!dcpl) {
    return nullptr;
  }
  */
  auto ds = h5d::open(loc, name, cq, hdf_store);
  if (!ds) {
    return nullptr;
  }
  // todo: With these changes, return ::ptr directly.  Also in 2d case.
  auto ret = new h5d_chunked_1d<T>(loc, name, move(*ds));
  return ptr(ret);
}

template <typename T>
h5d_chunked_1d<T>::h5d_chunked_1d(hid_t loc, string name, h5d ds)
    : ds(move(ds)), dsp_wr(this->ds) {}

template <typename T>
h5d_chunked_1d<T>::h5d_chunked_1d(h5d_chunked_1d &&x)
    : ds(move(x.ds)), dsp_wr(move(x.dsp_wr)) {}

template <typename T> h5d_chunked_1d<T>::~h5d_chunked_1d() {
  LOG(7, "~h5d_chunked_1d  count_append_calls: {}, count_append_bytes: {}, "
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
      LOG(3, "fail buffer");
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
      LOG(3, "unhandled error");
      exit(1);
    }
  }
  if (!do_buf) {
    auto res = ds.append_data_1d(data, nlen);
    if (res.status == AppendResult::ERROR) {
      return {res};
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
    return wr.status;
  }
  buf_n = 0;
  return AppendResult::OK;
}

template <typename T>
typename h5d_chunked_2d<T>::ptr
h5d_chunked_2d<T>::create(hid_t loc, string name, hsize_t ncols,
                          hsize_t chunk_bytes, CollectiveQueue *cq) {
  auto dsp = h5s::simple_unlim<2>({{0, ncols}});
  if (!dsp) {
    return nullptr;
  }
  auto dcpl = h5p::dataset_create::chunked2(nat_type<T>(), ncols, chunk_bytes);
  if (!dcpl) {
    return nullptr;
  }
  auto ds = h5d::create(loc, name, nat_type<T>(), move(*dsp), move(*dcpl), cq);
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
  // TODO
  // Adapt to the 1d case.
  LOG(3, "not supported currently");
  exit(1);
  append_ret ret{AppendResult::ERROR};
  if (nlen != dsp_wr.sini.at(1)) {
    return {AppendResult::ERROR};
  }
  bool do_buf = nlen * sizeof(T) < 4 * 1024;
  if (do_buf) {
    std::copy(data, data + nlen, std::back_inserter(buf));
    buf_bytes += nlen * sizeof(T);
  }
  if (buf_bytes > 128 * 1024 || (!do_buf && buf_bytes > 0)) {
    if (flush_buf() != 0) {
      return {AppendResult::ERROR};
    }
  }
  if (!do_buf) {
    ret = ds.append_data_2d(data, nlen);
    if (!ret) {
      return ret;
    }
  }
  ret.status = AppendResult::OK;
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

template h5d_chunked_1d<uint32_t>::ptr
h5d_chunked_1d<uint32_t>::open(hid_t loc, string name, CollectiveQueue *cq,
                               HDFIDStore *hdf_store);
template h5d_chunked_1d<uint64_t>::ptr
h5d_chunked_1d<uint64_t>::open(hid_t loc, string name, CollectiveQueue *cq,
                               HDFIDStore *hdf_store);

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
