#pragma once

#include "CollectiveQueue.h"
#include <array>
#include <h5cpp/hdf5.hpp>
#include <hdf5.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace h5 {

class RuntimeError : public std::runtime_error {
public:
  RuntimeError(std::string const &x) : std::runtime_error(x) {}
};

void swap(hsize_t &, hsize_t &);

enum class AppendResult : uint32_t {
  OK,
  ERROR,
};

struct append_ret {
  AppendResult status;
  uint64_t written_bytes;
  uint64_t ix0;
  operator bool() const { return status == AppendResult::OK; }
};

class h5d {
public:
  typedef std::unique_ptr<h5d> ptr;
  static ptr create(hdf5::node::Group loc, std::string name,
                    hdf5::datatype::Datatype Type, hdf5::dataspace::Simple dsp,
                    hdf5::property::DatasetCreationList dcpl,
                    CollectiveQueue *cq);
  static ptr open_single(hdf5::node::Group loc, std::string name,
                         CollectiveQueue *cq, HDFIDStore *hdf_store);
  static ptr open(hdf5::node::Group loc, std::string name, CollectiveQueue *cq,
                  HDFIDStore *hdf_store);
  h5d(h5d &&x);
  ~h5d();
  friend void swap(h5d &x, h5d &y);
  void lookup_cqsnowix(char const *ds_name, size_t &cqsnowix);
  template <typename T> append_ret append_data_1d(T const *data, hsize_t nlen);
  template <typename T> append_ret append_data_2d(T const *data, hsize_t nlen);
  std::string name;
  hdf5::node::Dataset Dataset;
  hdf5::datatype::Datatype Type;
  hdf5::property::DatasetTransferList PLTransfer;
  int ndims = -1;
  hdf5::dataspace::Simple DSPMem;
  hdf5::dataspace::Simple DSPTgt;
  hdf5::Dimensions snow;
  hdf5::Dimensions smax;
  hdf5::Dimensions sext;
  CollectiveQueue *cq = nullptr;
  HDFIDStore *hdf_store = nullptr;
  int mpi_rank = -1;

private:
  h5d();
  void init_basics();
  uint64_t TotalNanosecondsSpent = 0;
};

template <typename T> class h5d_chunked_1d;
template <typename T> void swap(h5d_chunked_1d<T> &x, h5d_chunked_1d<T> &y);

template <typename T> class h5d_chunked_1d {
public:
  typedef std::unique_ptr<h5d_chunked_1d<T>> ptr;
  static ptr create(hdf5::node::Group loc, std::string name,
                    hsize_t chunk_bytes, CollectiveQueue *cq);
  static ptr open(hdf5::node::Group loc, std::string name, CollectiveQueue *cq,
                  HDFIDStore *hdf_store);
  h5d ds;
  h5d_chunked_1d(h5d_chunked_1d &&x);
  ~h5d_chunked_1d();
  friend void swap<>(h5d_chunked_1d &x, h5d_chunked_1d &y);
  append_ret append_data_1d(T const *data, hsize_t nlen);
  AppendResult flush_buf();
  void buffer_init(size_t buf_size, size_t buf_packet_max);

private:
  h5d_chunked_1d(std::string name, h5d ds);
  hdf5::dataspace::Simple dsp_wr;
  size_t buf_size = 0;
  size_t buf_packet_max = 0;
  size_t buf_n = 0;
  std::vector<char> buf;
  hsize_t i0 = 0;
  uint64_t count_buffer_copy_calls = 0;
  uint64_t count_buffer_copy_bytes = 0;
  uint64_t count_append_calls = 0;
  uint64_t count_append_bytes = 0;
};

template <typename T> class h5d_chunked_2d;
template <typename T> void swap(h5d_chunked_2d<T> &x, h5d_chunked_2d<T> &y);

template <typename T> class h5d_chunked_2d {
public:
  typedef std::unique_ptr<h5d_chunked_2d<T>> ptr;
  static ptr create(hdf5::node::Group loc, std::string name, hsize_t ncols,
                    hsize_t chunk_bytes, CollectiveQueue *cq);
  static ptr open(hdf5::node::Group loc, std::string name, hsize_t ncols,
                  CollectiveQueue *cq, HDFIDStore *hdf_store);
  h5d ds;
  h5d_chunked_2d(h5d_chunked_2d &&x);
  ~h5d_chunked_2d();
  friend void swap<>(h5d_chunked_2d &x, h5d_chunked_2d &y);
  append_ret append_data_2d(T const *data, hsize_t nlen);
  AppendResult flush_buf();
  void buffer_init(size_t buf_size, size_t buf_packet_max);

private:
  h5d_chunked_2d(std::string name, h5d ds, hsize_t ncols);
  hdf5::dataspace::Simple dsp_wr;
  hsize_t ncols;
  size_t buf_size = 0;
  size_t buf_packet_max = 0;
  size_t buf_n = 0;
  std::vector<T> buf;
  uint32_t buf_bytes = 0;
  hsize_t i0 = 0;
  uint64_t count_buffer_copy_calls = 0;
  uint64_t count_buffer_copy_bytes = 0;
  uint64_t count_append_calls = 0;
  uint64_t count_append_bytes = 0;
};

} // namespace h5
