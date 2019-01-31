#include "h5.h"
#include "logger.h"
#include <chrono>

namespace h5 {

void swap(hsize_t &x, hsize_t &y) {
  x ^= y;
  y ^= x;
  x ^= y;
}

void h5d::init_basics() {
  Type = Dataset.datatype();
  DSPTgt = Dataset.dataspace();
  ndims = DSPTgt.rank();
  LOG(spdlog::level::trace, "h5d::init_basics");
  ShapeNow = hdf5::Dimensions(ndims, 0);
  sext = DSPTgt.current_dimensions();
  ShapeMax = DSPTgt.maximum_dimensions();
  for (int i1 = 0; i1 < ndims; ++i1) {
    LOG(spdlog::level::trace, "{:20} i: {}  sext: {:21}  ShapeMax: {:21}", Name, i1,
        sext.at(i1), ShapeMax.at(i1));
    try {
      DSPMem = hdf5::dataspace::Simple({0, 0}, {H5S_UNLIMITED, H5S_UNLIMITED});
    } catch (std::runtime_error const &e) {
      std::throw_with_nested(
          RuntimeError("hdf5::dataspace::Simple ctor failure"));
    }
    PLTransfer = hdf5::property::DatasetTransferList();
  }
}

h5d::ptr h5d::create(hdf5::node::Group const &Node, std::string const &Name,
                     hdf5::datatype::Datatype const &Type,
                     hdf5::dataspace::Simple const &dsp,
                     hdf5::property::DatasetCreationList const &dcpl) {
  try {
    // Creation is done in single process mode.
    // Can do set_extent here.
    auto ret = std::unique_ptr<h5d>(new h5d);
    auto &o = *ret;
    o.Type = Type;
    o.Name = Name;
    {
      // Use NULL as fill value.
      // Looks weird because we work around the current h5cpp API.
      int *fillptr = nullptr;
      dcpl.fill_value(*fillptr, Type);
    }
    o.Dataset = Node.create_dataset(Name, o.Type, dsp, dcpl);
    o.init_basics();
    return ret;
  } catch (...) {
    return {nullptr};
  }
}

h5d::ptr h5d::open_single(hdf5::node::Group const &Node,
                          std::string const &Name) {
  // Open in single-process mode
  auto ret = std::unique_ptr<h5d>(new h5d);
  auto &o = *ret;
  o.Name = Name;
  o.Dataset = Node.get_dataset(Name);
  o.init_basics();
  return ret;
}

h5d::ptr h5d::open(hdf5::node::Group const &Node, std::string const &Name) {
  return open_single(Node, Name);
}

h5d::h5d(h5d &&x) noexcept { swap(*this, x); }

h5d::~h5d() {
  LOG(spdlog::level::trace, "~h5d ds");
  if (Dataset.is_valid()) {
    Dataset.extent(ShapeNow);
  }
}

void swap(h5d &x, h5d &y) {
  using std::swap;
  swap(x.Name, y.Name);
  swap(x.Type, y.Type);
  swap(x.PLTransfer, y.PLTransfer);
  swap(x.Dataset, y.Dataset);
  swap(x.ndims, y.ndims);
  swap(x.DSPMem, y.DSPMem);
  swap(x.DSPTgt, y.DSPTgt);
  swap(x.ShapeNow, y.ShapeNow);
  swap(x.ShapeMax, y.ShapeMax);
  swap(x.sext, y.sext);
  swap(x.mpi_rank, y.mpi_rank);
}

template <typename T>
append_ret h5d::append_data_1d(T const *data, hsize_t nlen) {
  using CLK = std::chrono::steady_clock;
  using MS = std::chrono::milliseconds;
  auto t1 = CLK::now();
  LOG(spdlog::level::trace, "append_data_{}d", ndims);
  auto ds_name = static_cast<std::string>(Dataset.link().path());
    LOG(spdlog::level::trace, "append_data_1d {} for dataset {}", nlen, ds_name);


  for (size_t i = 1; i < sext.size(); ++i) {
    sext[i] = ShapeMax[i];
  }

  hsize_t nlen_0 = nlen;
  if (ndims == 2) {
    if (nlen % sext[1] != 0) {
      LOG(spdlog::level::err, "dataset dimensions do not match");
      return {AppendResult::ERROR};
    }
    nlen_0 /= sext[1];
  }

  size_t snext = -1;
  snext = ShapeNow[0];

  if (snext + nlen_0 > sext[0]) {
    t1 = CLK::now();
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
        LOG(spdlog::level::err, "snow_1_ln2 >= BLOCK; {} >= {};  sext[1]: {}",
            snow_1_ln2, BLOCK, sext[1]);
        snow_1_ln2 = BLOCK - 1;
      }
      BLOCK -= snow_1_ln2;
    }
    uint32_t const MAX = BLOCK + 8;
    hdf5::Dimensions sext2;
    sext2 = sext;
    LOG(spdlog::level::trace, "Before extending: {}  min target: {}", sext2.at(0),
        snext + nlen_0);
    sext2[0] = (1 + (((snext + nlen_0) * 4 / 3) >> BLOCK)) << BLOCK;
    if (sext2[0] - sext[0] > (1u << MAX)) {
      sext2[0] = sext[0] + (1 << MAX);
    }
    if (sext.size() == 1) {
      LOG(spdlog::level::trace,
          "snext: {:12}  set_extent  d: 1\n  from: {:12}  to: {:12}", snext,
          sext.at(0), sext2.at(0));
    } else if (sext.size() == 2) {
      LOG(spdlog::level::trace,
          "snext: {:12}  set_extent  d: 2\n  from: {:12}  to: {:12}\n  "
          "from: {:12}  to: {:12}",
          snext, sext.at(0), sext2.at(0), sext.at(1), sext2.at(1));
    } else {
      LOG(spdlog::level::trace, "snext: {:12}  set_extent  d: {}   NOT SUPPORTED",
          sext.size());
    }

    auto t2 = CLK::now();

    try {
      Dataset.extent(sext2);
    } catch (...) {
      LOG(spdlog::level::err, "Dataset.extent()");
      return {AppendResult::ERROR};
    }
    DSPTgt = Dataset.dataspace();
    sext = DSPTgt.current_dimensions();
    ShapeMax = DSPTgt.maximum_dimensions();

    for (size_t i = 1; i < sext.size(); ++i) {
      sext.at(i) = sext2.at(i);
    }
    auto t3 = CLK::now();
    LOG(spdlog::level::trace, "h5d::append_data_1d set_extent: {} + {}",
        std::chrono::duration_cast<MS>(t2 - t1).count(),
        std::chrono::duration_cast<MS>(t3 - t2).count());
  }

    LOG(spdlog::level::trace, "try to get the dsp dims:");
    auto sext = DSPTgt.current_dimensions();
    auto smax = DSPTgt.maximum_dimensions();
    for (int i1 = 0; i1 < ndims; ++i1) {
      LOG(spdlog::level::trace, "dimensions: {:20} {}: {:21} {:21}", Name, i1,
          sext.at(i1), smax.at(i1));
    }

  {
    hdf5::Dimensions offset(sext.size()), block(sext.size()),
        count(sext.size()), stride(sext.size());
    offset[0] = 0;
    block[0] = 1;
    count[0] = nlen_0;
    stride[0] = 1;
    for (size_t i = 1; i < offset.size(); ++i) {
      offset[i] = 0;
      block[i] = 1;
      count[i] = sext[i];
      stride[i] = 1;
    }
    try {
      DSPMem.dimensions(count, count);
      DSPMem.selection(
          hdf5::dataspace::SelectionOperation::SET,
          hdf5::dataspace::Hyperslab(offset, block, count, stride));
    } catch (...) {
      LOG(spdlog::level::err, "can not select mem hyperslab");
      return {AppendResult::ERROR};
    }
  }

  hdf5::Dimensions tgt_offset(sext.size()), tgt_block(sext.size()),
      tgt_count(sext.size()), tgt_stride(sext.size());
  tgt_offset[0] = snext;
  tgt_block[0] = 1;
  tgt_count[0] = nlen_0;
  tgt_stride[0] = 1;
  for (size_t i = 1; i < tgt_offset.size(); ++i) {
    tgt_offset[i] = 0;
    tgt_block[i] = 1;
    tgt_count[i] = sext[1];
    tgt_stride[i] = 1;
  }
    for (int i1 = 0; i1 < ndims; ++i1) {
      LOG(spdlog::level::trace, "select tgt  i1: {}  start: {}  count: {}", i1,
          tgt_offset.at(i1), tgt_count.at(i1));
    }
  DSPTgt.selection(
      hdf5::dataspace::SelectionOperation::SET,
      hdf5::dataspace::Hyperslab(tgt_offset, tgt_block, tgt_count, tgt_stride));
  auto t2 = CLK::now();
  try {
    Dataset.write(*data, Type, DSPMem, DSPTgt, PLTransfer);
  } catch (...) {
    LOG(spdlog::level::trace, "write failed  ds_name: {}", ds_name);
      auto dsp = hdf5::dataspace::Simple(Dataset.dataspace());
      auto sext = dsp.current_dimensions();
      auto smax = dsp.current_dimensions();
      for (int i1 = 0; i1 < ndims; ++i1) {
        LOG(spdlog::level::trace, "dimensions {}: {:12} {:12}", i1, sext.at(i1),
            smax.at(i1));
      }
    return {AppendResult::ERROR};
  }
  ShapeNow[0] = snext + nlen_0;
  for (size_t i = 1; i < ShapeNow.size(); ++i) {
    ShapeNow[i] = sext[i];
  }
  auto t3 = CLK::now();
  auto dt1 = std::chrono::duration_cast<MS>(t2 - t1).count();
  auto dt2 = std::chrono::duration_cast<MS>(t3 - t2).count();
  TotalNanosecondsSpent += dt1 + dt2;
  return {AppendResult::OK, sizeof(T) * nlen, tgt_offset[0]};
}

append_ret h5d::append(std::string const &String) {
  try {
    if (!Dataset.is_valid()) {
      LOG(spdlog::level::err, "Dataset is not valid");
      return {AppendResult::ERROR};
    }
    {
      auto Type = Dataset.datatype();
      auto TypeHid = static_cast<hid_t>(Type);
      if (H5Tget_class(TypeHid) != H5T_STRING or
          H5Tis_variable_str(TypeHid) == 0) {
        LOG(spdlog::level::err, "Unexpected datatype");
        return {AppendResult::ERROR};
      }
    }
    hdf5::dataspace::Simple SpaceTarget(Dataset.dataspace());
    auto CurrentDimensions = SpaceTarget.current_dimensions();
    CurrentDimensions.at(0) += 1;
    Dataset.extent(CurrentDimensions);
    SpaceTarget = Dataset.dataspace();
    CurrentDimensions = SpaceTarget.current_dimensions();
    hdf5::dataspace::Simple SpaceMemory;
    SpaceMemory.dimensions({1});
    SpaceMemory.selection.all();
    hdf5::dataspace::Hyperslab TargetSlab(
        {SpaceTarget.current_dimensions().at(0) - 1}, {1}, {1}, {1});
    SpaceTarget.selection(hdf5::dataspace::SelectionOperation::SET, TargetSlab);
    Dataset.write(String, Dataset.datatype(), SpaceMemory, SpaceTarget);
    ShapeNow =
        hdf5::dataspace::Simple(Dataset.dataspace()).current_dimensions();
    return {AppendResult::OK, String.size(), 0};
  } catch (std::runtime_error const &e) {
    LOG(spdlog::level::err, "exception while writing: {}", e.what());
    return {AppendResult::ERROR};
  }
}

template <typename T>
append_ret h5d::append_data_2d(T const *data, hsize_t nlen) {
  return append_data_1d(data, nlen);
}

template <typename T>
typename h5d_chunked_1d<T>::ptr
h5d_chunked_1d<T>::create(hdf5::node::Group const &loc, std::string name,
                          hsize_t chunk_bytes) {
  hdf5::dataspace::Simple dsp({0}, {H5S_UNLIMITED});
  hdf5::property::DatasetCreationList dcpl;
  auto Type = hdf5::datatype::create<T>().native_type();
  hsize_t MimimumChunkSize = 1024;
  dcpl.chunk({std::max<hsize_t>(MimimumChunkSize, chunk_bytes / Type.size())});
  auto ds = h5d::create(loc, name, Type, dsp, dcpl);
  if (!ds) {
    return nullptr;
  }
  // todo: With these changes, return ::ptr directly.  Also in 2d case.
  auto ret = new h5d_chunked_1d<T>(name, std::move(*ds));
  return ptr(ret);
}

template <typename T>
typename h5d_chunked_1d<T>::ptr
h5d_chunked_1d<T>::open(hdf5::node::Group const &loc, std::string name) {
  auto ds = h5d::open(loc, name);
  if (!ds) {
    return ptr();
  }
  return ptr(new h5d_chunked_1d<T>(name, std::move(*ds)));
}

template <typename T>

h5d_chunked_1d<T>::h5d_chunked_1d(std::string const &, h5d ds_)
    : DataSet(std::move(ds_)) {
  if (!DataSet.Dataset.is_valid()) {
    LOG(spdlog::level::critical, "not a dataset");
    throw std::runtime_error("Not a dataset");
  }
  auto dsp = hdf5::dataspace::Simple(DataSet.Dataset.dataspace());
  auto ndims = dsp.rank();
  if (ndims != 1) {
    auto msg = fmt::format(
        "wrong dimension Dataset: {}  ndims: {}",
        static_cast<std::string>(DataSet.Dataset.link().path()), ndims);
    LOG(spdlog::level::critical, "{}", msg);
    throw std::runtime_error(msg);
  }
  dsp_wr = hdf5::dataspace::Simple(dsp);
}

template <typename T>
h5d_chunked_1d<T>::h5d_chunked_1d(h5d_chunked_1d &&x) noexcept
    : DataSet(std::move(x.DataSet)), dsp_wr(std::move(x.dsp_wr)) {}

template <typename T> h5d_chunked_1d<T>::~h5d_chunked_1d() {
  LOG(spdlog::level::trace,
      "~h5d_chunked_1d  count_append_calls: {}, count_append_bytes: {}, "
      "count_buffer_copy_calls: {}, count_buffer_copy_bytes: {}",
      count_append_calls, count_append_bytes, count_buffer_copy_calls,
      count_buffer_copy_bytes);
  flush_buf();
}

template <typename T> void swap(h5d_chunked_1d<T> &x, h5d_chunked_1d<T> &y) {
  swap(x.DataSet, y.DataSet);
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
      LOG(spdlog::level::err, "fail buffer");
      exit(1);
    }
    auto p1 = reinterpret_cast<const char *>(data);
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
      LOG(spdlog::level::err, "unhandled error");
      exit(1);
    }
  }
  if (!do_buf) {
    auto res = DataSet.append_data_1d(data, nlen);
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
  auto wr = DataSet.append_data_1d(reinterpret_cast<T *>(buf.data()),
                                   buf_n / sizeof(T));
  if (wr.status != AppendResult::OK) {
    LOG(spdlog::level::trace, "FLUSH NOT OK");
    return wr.status;
  }
  buf_n = 0;
  return AppendResult::OK;
}

template <typename T> size_t h5d_chunked_1d<T>::size() const {
  return DataSet.ShapeNow.at(0);
}

Chunked1DString::Chunked1DString(h5d ds) : DataSet(std::move(ds)) {}

Chunked1DString::ptr Chunked1DString::create(hdf5::node::Group const &Node,
                                             std::string const &Name,
                                             hsize_t ChunkBytes) {
  hdf5::dataspace::Simple Space({0}, {H5S_UNLIMITED});
  hdf5::property::DatasetCreationList dcpl;
  auto Type = hdf5::datatype::String::variable();
  Type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  hsize_t MimimumChunkSize = 1024;
  dcpl.chunk({std::max<hsize_t>(MimimumChunkSize, ChunkBytes)});
  auto ds = h5d::create(Node, Name, Type, Space, dcpl);
  if (!ds) {
    return nullptr;
  }
  auto ret = new Chunked1DString(std::move(*ds));
  return ptr(ret);
}

Chunked1DString::ptr Chunked1DString::open(hdf5::node::Group const &Node,
                                           std::string const &Name) {
  auto ds = h5d::open(Node, Name);
  if (!ds) {
    LOG(spdlog::level::err, "Could not open dataset: {}", Name);
    return ptr();
  }
  return ptr(new Chunked1DString(std::move(*ds)));
}

append_ret Chunked1DString::append(std::string const &String) {
  return DataSet.append(String);
}

template <typename T>
typename h5d_chunked_2d<T>::ptr
h5d_chunked_2d<T>::create(hdf5::node::Group const &loc, std::string const &name,
                          hsize_t NrOfColumns, hsize_t chunk_bytes) {
  hdf5::dataspace::Simple dsp({0, NrOfColumns}, {H5S_UNLIMITED, NrOfColumns});
  hdf5::property::DatasetCreationList dcpl;
  auto Type = hdf5::datatype::create<T>().native_type();
  dcpl.chunk({std::max<hsize_t>(1, chunk_bytes / NrOfColumns / Type.size()),
              NrOfColumns});
  auto ds = h5d::create(loc, name, Type, dsp, dcpl);
  if (!ds) {
    return nullptr;
  }
  auto ret = new h5d_chunked_2d<T>(name, std::move(*ds), NrOfColumns);
  return ptr(ret);
}

template <typename T>
typename h5d_chunked_2d<T>::ptr
h5d_chunked_2d<T>::open(hdf5::node::Group const &loc, std::string const &name,
                        hsize_t NrOfColumns) {
  auto ds = h5d::open(loc, name);
  if (!ds) {
    return ptr();
  }
  return ptr(new h5d_chunked_2d<T>(name, std::move(*ds), NrOfColumns));
}

template <typename T>

h5d_chunked_2d<T>::h5d_chunked_2d(std::string const &, h5d LinkedDataSet,
                                  hsize_t NrOfColumns)
    : DataSet(std::move(LinkedDataSet)), NrOfColumns_(NrOfColumns) {
  if (!DataSet.Dataset.is_valid()) {
    LOG(spdlog::level::critical, "not a dataset");
    throw std::runtime_error("Not a dataset");
  }
  auto dsp = hdf5::dataspace::Simple(DataSet.Dataset.dataspace());
  auto ndims = dsp.rank();
  if (ndims != 2) {
    auto msg = fmt::format(
        "wrong dimension Dataset: {}  ndims: {}",
        static_cast<std::string>(DataSet.Dataset.link().path()), ndims);
    LOG(spdlog::level::critical, "{}", msg);
    throw std::runtime_error(msg);
  }
  dsp_wr = hdf5::dataspace::Simple(dsp);
}

template <typename T>
h5d_chunked_2d<T>::h5d_chunked_2d(h5d_chunked_2d &&x) noexcept
    : DataSet(std::move(x.DataSet)), dsp_wr(std::move(x.dsp_wr)) {}

template <typename T> h5d_chunked_2d<T>::~h5d_chunked_2d() { flush_buf(); }

template <typename T> void swap(h5d_chunked_2d<T> &x, h5d_chunked_2d<T> &y) {
  using std::swap;
  swap(x.DataSet, y.DataSet);
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
      LOG(spdlog::level::err, "fail buffer");
      exit(1);
    }
    auto p1 = reinterpret_cast<const char *>(data);
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
      LOG(spdlog::level::err, "unhandled error");
      exit(1);
    }
  }
  if (!do_buf) {
    auto res = DataSet.append_data_1d(data, nlen);
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
  auto wr = DataSet.append_data_2d(reinterpret_cast<T *>(buf.data()),
                                   buf_n / sizeof(T));
  if (wr.status != AppendResult::OK) {
    return wr.status;
  }
  buf_n = 0;
  return AppendResult::OK;
}

template <typename T> size_t h5d_chunked_2d<T>::size() const {
  return DataSet.ShapeNow.at(0);
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

template h5d_chunked_1d< uint8_t>::ptr h5d_chunked_1d< uint8_t>::create(hdf5::node::Group const &loc, std::string name, hsize_t chunk_bytes);
template h5d_chunked_1d<uint16_t>::ptr h5d_chunked_1d<uint16_t>::create(hdf5::node::Group const &loc, std::string name, hsize_t chunk_bytes);
template h5d_chunked_1d<uint32_t>::ptr h5d_chunked_1d<uint32_t>::create(hdf5::node::Group const &loc, std::string name, hsize_t chunk_bytes);
template h5d_chunked_1d<uint64_t>::ptr h5d_chunked_1d<uint64_t>::create(hdf5::node::Group const &loc, std::string name, hsize_t chunk_bytes);
template h5d_chunked_1d<  int8_t>::ptr h5d_chunked_1d<  int8_t>::create(hdf5::node::Group const &loc, std::string name, hsize_t chunk_bytes);
template h5d_chunked_1d< int16_t>::ptr h5d_chunked_1d< int16_t>::create(hdf5::node::Group const &loc, std::string name, hsize_t chunk_bytes);
template h5d_chunked_1d< int32_t>::ptr h5d_chunked_1d< int32_t>::create(hdf5::node::Group const &loc, std::string name, hsize_t chunk_bytes);
template h5d_chunked_1d< int64_t>::ptr h5d_chunked_1d< int64_t>::create(hdf5::node::Group const &loc, std::string name, hsize_t chunk_bytes);
template h5d_chunked_1d<   float>::ptr h5d_chunked_1d<   float>::create(hdf5::node::Group const &loc, std::string name, hsize_t chunk_bytes);
template h5d_chunked_1d<  double>::ptr h5d_chunked_1d<  double>::create(hdf5::node::Group const &loc, std::string name, hsize_t chunk_bytes);

template h5d_chunked_1d< uint8_t>::ptr h5d_chunked_1d< uint8_t>::open(hdf5::node::Group const &loc, std::string name);
template h5d_chunked_1d<uint16_t>::ptr h5d_chunked_1d<uint16_t>::open(hdf5::node::Group const &loc, std::string name);
template h5d_chunked_1d<uint32_t>::ptr h5d_chunked_1d<uint32_t>::open(hdf5::node::Group const &loc, std::string name);
template h5d_chunked_1d<uint64_t>::ptr h5d_chunked_1d<uint64_t>::open(hdf5::node::Group const &loc, std::string name);
template h5d_chunked_1d<  int8_t>::ptr h5d_chunked_1d<  int8_t>::open(hdf5::node::Group const &loc, std::string name);
template h5d_chunked_1d< int16_t>::ptr h5d_chunked_1d< int16_t>::open(hdf5::node::Group const &loc, std::string name);
template h5d_chunked_1d< int32_t>::ptr h5d_chunked_1d< int32_t>::open(hdf5::node::Group const &loc, std::string name);
template h5d_chunked_1d< int64_t>::ptr h5d_chunked_1d< int64_t>::open(hdf5::node::Group const &loc, std::string name);
template h5d_chunked_1d<   float>::ptr h5d_chunked_1d<   float>::open(hdf5::node::Group const &loc, std::string name);
template h5d_chunked_1d<  double>::ptr h5d_chunked_1d<  double>::open(hdf5::node::Group const &loc, std::string name);

template h5d_chunked_1d< uint8_t>::h5d_chunked_1d(std::string const &name, h5d ds);
template h5d_chunked_1d<uint16_t>::h5d_chunked_1d(std::string const &name, h5d ds);
template h5d_chunked_1d<uint32_t>::h5d_chunked_1d(std::string const &name, h5d ds);
template h5d_chunked_1d<uint64_t>::h5d_chunked_1d(std::string const &name, h5d ds);
template h5d_chunked_1d<  int8_t>::h5d_chunked_1d(std::string const &name, h5d ds);
template h5d_chunked_1d< int16_t>::h5d_chunked_1d(std::string const &name, h5d ds);
template h5d_chunked_1d< int32_t>::h5d_chunked_1d(std::string const &name, h5d ds);
template h5d_chunked_1d< int64_t>::h5d_chunked_1d(std::string const &name, h5d ds);
template h5d_chunked_1d<   float>::h5d_chunked_1d(std::string const &name, h5d ds);
template h5d_chunked_1d<  double>::h5d_chunked_1d(std::string const &name, h5d ds);

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

template AppendResult h5d_chunked_1d< uint8_t>::flush_buf();
template AppendResult h5d_chunked_1d<uint16_t>::flush_buf();
template AppendResult h5d_chunked_1d<uint32_t>::flush_buf();
template AppendResult h5d_chunked_1d<uint64_t>::flush_buf();
template AppendResult h5d_chunked_1d<  int8_t>::flush_buf();
template AppendResult h5d_chunked_1d< int16_t>::flush_buf();
template AppendResult h5d_chunked_1d< int32_t>::flush_buf();
template AppendResult h5d_chunked_1d< int64_t>::flush_buf();
template AppendResult h5d_chunked_1d<   float>::flush_buf();
template AppendResult h5d_chunked_1d<  double>::flush_buf();

template AppendResult h5d_chunked_2d< uint8_t>::flush_buf();
template AppendResult h5d_chunked_2d<uint16_t>::flush_buf();
template AppendResult h5d_chunked_2d<uint32_t>::flush_buf();
template AppendResult h5d_chunked_2d<uint64_t>::flush_buf();
template AppendResult h5d_chunked_2d<  int8_t>::flush_buf();
template AppendResult h5d_chunked_2d< int16_t>::flush_buf();
template AppendResult h5d_chunked_2d< int32_t>::flush_buf();
template AppendResult h5d_chunked_2d< int64_t>::flush_buf();
template AppendResult h5d_chunked_2d<   float>::flush_buf();
template AppendResult h5d_chunked_2d<  double>::flush_buf();

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

template append_ret h5d_chunked_1d< uint8_t>::h5d_chunked_1d::append_data_1d( uint8_t const *data, hsize_t nlen);
template append_ret h5d_chunked_1d<uint16_t>::h5d_chunked_1d::append_data_1d(uint16_t const *data, hsize_t nlen);
template append_ret h5d_chunked_1d<uint32_t>::h5d_chunked_1d::append_data_1d(uint32_t const *data, hsize_t nlen);
template append_ret h5d_chunked_1d<uint64_t>::h5d_chunked_1d::append_data_1d(uint64_t const *data, hsize_t nlen);
template append_ret h5d_chunked_1d<  int8_t>::h5d_chunked_1d::append_data_1d(  int8_t const *data, hsize_t nlen);
template append_ret h5d_chunked_1d< int16_t>::h5d_chunked_1d::append_data_1d( int16_t const *data, hsize_t nlen);
template append_ret h5d_chunked_1d< int32_t>::h5d_chunked_1d::append_data_1d( int32_t const *data, hsize_t nlen);
template append_ret h5d_chunked_1d< int64_t>::h5d_chunked_1d::append_data_1d( int64_t const *data, hsize_t nlen);
template append_ret h5d_chunked_1d<   float>::h5d_chunked_1d::append_data_1d(   float const *data, hsize_t nlen);
template append_ret h5d_chunked_1d<  double>::h5d_chunked_1d::append_data_1d(  double const *data, hsize_t nlen);

template size_t h5d_chunked_1d< uint8_t>::h5d_chunked_1d::size() const;
template size_t h5d_chunked_1d<uint16_t>::h5d_chunked_1d::size() const;
template size_t h5d_chunked_1d<uint32_t>::h5d_chunked_1d::size() const;
template size_t h5d_chunked_1d<uint64_t>::h5d_chunked_1d::size() const;
template size_t h5d_chunked_1d<  int8_t>::h5d_chunked_1d::size() const;
template size_t h5d_chunked_1d< int16_t>::h5d_chunked_1d::size() const;
template size_t h5d_chunked_1d< int32_t>::h5d_chunked_1d::size() const;
template size_t h5d_chunked_1d< int64_t>::h5d_chunked_1d::size() const;
template size_t h5d_chunked_1d<   float>::h5d_chunked_1d::size() const;
template size_t h5d_chunked_1d<  double>::h5d_chunked_1d::size() const;

template h5d_chunked_2d< uint8_t>::ptr h5d_chunked_2d< uint8_t>::create(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns, hsize_t chunk_bytes);
template h5d_chunked_2d<uint16_t>::ptr h5d_chunked_2d<uint16_t>::create(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns, hsize_t chunk_bytes);
template h5d_chunked_2d<uint32_t>::ptr h5d_chunked_2d<uint32_t>::create(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns, hsize_t chunk_bytes);
template h5d_chunked_2d<uint64_t>::ptr h5d_chunked_2d<uint64_t>::create(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns, hsize_t chunk_bytes);
template h5d_chunked_2d<  int8_t>::ptr h5d_chunked_2d<  int8_t>::create(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns, hsize_t chunk_bytes);
template h5d_chunked_2d< int16_t>::ptr h5d_chunked_2d< int16_t>::create(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns, hsize_t chunk_bytes);
template h5d_chunked_2d< int32_t>::ptr h5d_chunked_2d< int32_t>::create(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns, hsize_t chunk_bytes);
template h5d_chunked_2d< int64_t>::ptr h5d_chunked_2d< int64_t>::create(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns, hsize_t chunk_bytes);
template h5d_chunked_2d<   float>::ptr h5d_chunked_2d<   float>::create(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns, hsize_t chunk_bytes);
template h5d_chunked_2d<  double>::ptr h5d_chunked_2d<  double>::create(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns, hsize_t chunk_bytes);


template h5d_chunked_2d< uint8_t>::ptr h5d_chunked_2d< uint8_t>::open(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns);
template h5d_chunked_2d<uint16_t>::ptr h5d_chunked_2d<uint16_t>::open(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns);
template h5d_chunked_2d<uint32_t>::ptr h5d_chunked_2d<uint32_t>::open(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns);
template h5d_chunked_2d<uint64_t>::ptr h5d_chunked_2d<uint64_t>::open(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns);
template h5d_chunked_2d<  int8_t>::ptr h5d_chunked_2d<  int8_t>::open(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns);
template h5d_chunked_2d< int16_t>::ptr h5d_chunked_2d< int16_t>::open(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns);
template h5d_chunked_2d< int32_t>::ptr h5d_chunked_2d< int32_t>::open(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns);
template h5d_chunked_2d< int64_t>::ptr h5d_chunked_2d< int64_t>::open(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns);
template h5d_chunked_2d<   float>::ptr h5d_chunked_2d<   float>::open(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns);
template h5d_chunked_2d<  double>::ptr h5d_chunked_2d<  double>::open(hdf5::node::Group const &loc, std::string const &name, hsize_t NrOfColumns);


template h5d_chunked_2d< uint8_t>::h5d_chunked_2d(std::string const &name, h5d LinkedDataSet, hsize_t NrOfColumns);
template h5d_chunked_2d<uint16_t>::h5d_chunked_2d(std::string const &name, h5d LinkedDataSet, hsize_t NrOfColumns);
template h5d_chunked_2d<uint32_t>::h5d_chunked_2d(std::string const &name, h5d LinkedDataSet, hsize_t NrOfColumns);
template h5d_chunked_2d<uint64_t>::h5d_chunked_2d(std::string const &name, h5d LinkedDataSet, hsize_t NrOfColumns);
template h5d_chunked_2d<  int8_t>::h5d_chunked_2d(std::string const &name, h5d LinkedDataSet, hsize_t NrOfColumns);
template h5d_chunked_2d< int16_t>::h5d_chunked_2d(std::string const &name, h5d LinkedDataSet, hsize_t NrOfColumns);
template h5d_chunked_2d< int32_t>::h5d_chunked_2d(std::string const &name, h5d LinkedDataSet, hsize_t NrOfColumns);
template h5d_chunked_2d< int64_t>::h5d_chunked_2d(std::string const &name, h5d LinkedDataSet, hsize_t NrOfColumns);
template h5d_chunked_2d<   float>::h5d_chunked_2d(std::string const &name, h5d LinkedDataSet, hsize_t NrOfColumns);
template h5d_chunked_2d<  double>::h5d_chunked_2d(std::string const &name, h5d LinkedDataSet, hsize_t NrOfColumns);


template h5d_chunked_2d< uint8_t>::~h5d_chunked_2d();
template h5d_chunked_2d<uint16_t>::~h5d_chunked_2d();
template h5d_chunked_2d<uint32_t>::~h5d_chunked_2d();
template h5d_chunked_2d<uint64_t>::~h5d_chunked_2d();
template h5d_chunked_2d<  int8_t>::~h5d_chunked_2d();
template h5d_chunked_2d< int16_t>::~h5d_chunked_2d();
template h5d_chunked_2d< int32_t>::~h5d_chunked_2d();
template h5d_chunked_2d< int64_t>::~h5d_chunked_2d();
template h5d_chunked_2d<   float>::~h5d_chunked_2d();
template h5d_chunked_2d<  double>::~h5d_chunked_2d();

template append_ret h5d_chunked_2d< uint8_t>::h5d_chunked_2d::append_data_2d( uint8_t const *data, hsize_t nlen);
template append_ret h5d_chunked_2d<uint16_t>::h5d_chunked_2d::append_data_2d(uint16_t const *data, hsize_t nlen);
template append_ret h5d_chunked_2d<uint32_t>::h5d_chunked_2d::append_data_2d(uint32_t const *data, hsize_t nlen);
template append_ret h5d_chunked_2d<uint64_t>::h5d_chunked_2d::append_data_2d(uint64_t const *data, hsize_t nlen);
template append_ret h5d_chunked_2d<  int8_t>::h5d_chunked_2d::append_data_2d(  int8_t const *data, hsize_t nlen);
template append_ret h5d_chunked_2d< int16_t>::h5d_chunked_2d::append_data_2d( int16_t const *data, hsize_t nlen);
template append_ret h5d_chunked_2d< int32_t>::h5d_chunked_2d::append_data_2d( int32_t const *data, hsize_t nlen);
template append_ret h5d_chunked_2d< int64_t>::h5d_chunked_2d::append_data_2d( int64_t const *data, hsize_t nlen);
template append_ret h5d_chunked_2d<   float>::h5d_chunked_2d::append_data_2d(   float const *data, hsize_t nlen);
template append_ret h5d_chunked_2d<  double>::h5d_chunked_2d::append_data_2d(  double const *data, hsize_t nlen);

template size_t h5d_chunked_2d< uint8_t>::h5d_chunked_2d::size() const;
template size_t h5d_chunked_2d<uint16_t>::h5d_chunked_2d::size() const;
template size_t h5d_chunked_2d<uint32_t>::h5d_chunked_2d::size() const;
template size_t h5d_chunked_2d<uint64_t>::h5d_chunked_2d::size() const;
template size_t h5d_chunked_2d<  int8_t>::h5d_chunked_2d::size() const;
template size_t h5d_chunked_2d< int16_t>::h5d_chunked_2d::size() const;
template size_t h5d_chunked_2d< int32_t>::h5d_chunked_2d::size() const;
template size_t h5d_chunked_2d< int64_t>::h5d_chunked_2d::size() const;
template size_t h5d_chunked_2d<   float>::h5d_chunked_2d::size() const;
template size_t h5d_chunked_2d<  double>::h5d_chunked_2d::size() const;

// clang-format on

} // namespace h5
