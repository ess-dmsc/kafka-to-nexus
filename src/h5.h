#pragma once

#include "logger.h"
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
  explicit RuntimeError(std::string const &x) : std::runtime_error(x) {}
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
  // Heap allocation only in sad path, so it's fast.
  std::string ErrorString;
};

class h5d {
public:
  using ptr = std::unique_ptr<h5d>;
  static ptr create(hdf5::node::Group const &Node, std::string const &Name,
                    hdf5::datatype::Datatype const &Type,
                    hdf5::dataspace::Simple const &dsp,
                    hdf5::property::DatasetCreationList const &dcpl);
  static ptr open_single(hdf5::node::Group const &Node,
                         std::string const &Name);
  static ptr open(hdf5::node::Group const &Node, std::string const &Name);
  h5d(h5d &&x) noexcept;
  ~h5d();
  friend void swap(h5d &x, h5d &y);
  template <typename T> append_ret append_data_1d(T const *data, hsize_t nlen);
  template <typename T> append_ret append_data_2d(T const *data, hsize_t nlen);

  /// \brief Write a string to this dataset.
  ///
  /// Writes the given string to the dataset if the dataset can contain strings.
  append_ret append(std::string const &String);
  std::string Name;
  hdf5::node::Dataset Dataset;
  hdf5::datatype::Datatype Type;
  hdf5::property::DatasetTransferList PLTransfer;
  int ndims = -1;
  hdf5::dataspace::Simple DSPMem;
  hdf5::dataspace::Simple DSPTgt;
  hdf5::Dimensions ShapeNow;
  hdf5::Dimensions ShapeMax;
  hdf5::Dimensions sext;
  int mpi_rank = -1;

private:
  h5d() = default;
  void init_basics();
  uint64_t TotalNanosecondsSpent = 0;
  SharedLogger Logger = getLogger();
};

template <typename T> class h5d_chunked_1d;
template <typename T> void swap(h5d_chunked_1d<T> &x, h5d_chunked_1d<T> &y);

template <typename T> class h5d_chunked_1d {
public:
  using ptr = std::unique_ptr<h5d_chunked_1d<T>>;
  static ptr create(hdf5::node::Group const &loc, std::string name,
                    hsize_t chunk_bytes);
  static ptr open(hdf5::node::Group const &loc, std::string name);
  h5d DataSet;
  h5d_chunked_1d(h5d_chunked_1d &&x) noexcept;
  ~h5d_chunked_1d();
  friend void swap<>(h5d_chunked_1d &x, h5d_chunked_1d &y);
  append_ret append_data_1d(T const *data, hsize_t nlen);
  AppendResult flush_buf();
  void buffer_init(size_t buf_size, size_t buf_packet_max);
  size_t size() const;

private:
  h5d_chunked_1d(std::string const &name, h5d ds);
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
  SharedLogger Logger = getLogger();
};

/// Specialized chunked dataset for strings.
class Chunked1DString {
public:
  using ptr = std::unique_ptr<Chunked1DString>;
  static ptr create(hdf5::node::Group const &Node, std::string const &Name,
                    hsize_t ChunkBytes);
  static ptr open(hdf5::node::Group const &Node, std::string const &Name);
  append_ret append(std::string const &String);
  h5d DataSet;

private:
  explicit Chunked1DString(h5d ds);
};

template <typename T> class h5d_chunked_2d;
template <typename T> void swap(h5d_chunked_2d<T> &x, h5d_chunked_2d<T> &y);

template <typename T> class h5d_chunked_2d {
public:
  using ptr = std::unique_ptr<h5d_chunked_2d<T>>;
  static ptr create(hdf5::node::Group const &loc, std::string const &name,
                    hsize_t NrOfColumns, hsize_t chunk_bytes);
  static ptr open(hdf5::node::Group const &loc, std::string const &name,
                  hsize_t NrOfColumns);
  h5d DataSet;
  h5d_chunked_2d(h5d_chunked_2d &&x) noexcept;
  ~h5d_chunked_2d();
  friend void swap<>(h5d_chunked_2d &x, h5d_chunked_2d &y);
  append_ret append_data_2d(T const *data, hsize_t nlen);
  AppendResult flush_buf();
  void buffer_init(size_t buf_size, size_t buf_packet_max);
  size_t size() const;

private:
  h5d_chunked_2d(std::string const &name, h5d LinkedDataSet,
                 hsize_t NrOfColumns);
  hdf5::dataspace::Simple dsp_wr;
  hsize_t NrOfColumns_{0};
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
  SharedLogger Logger = getLogger();
};

} // namespace h5
