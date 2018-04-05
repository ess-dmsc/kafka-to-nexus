#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include "../../h5.h"
#include <array>
#include <flatbuffers/flatbuffers.h>
#include <memory>
#include <vector>

template <typename T> using uptr = std::unique_ptr<T>;

namespace FileWriter {
namespace Schemas {
namespace f142 {
#include "schemas/f142_logdata_generated.h"
using FBUF = LogData;

class writer_typed_base {
public:
  virtual ~writer_typed_base() = default;
  virtual h5::append_ret write_impl(FBUF const *fbuf) = 0;
};

template <typename DT, typename FV>
class writer_typed_array : public writer_typed_base {
public:
  writer_typed_array(hdf5::node::Group hdf_group,
                     std::string const &source_name, hsize_t ncols,
                     Value fb_value_type_id, CollectiveQueue *cq);
  writer_typed_array(hdf5::node::Group, std::string const &source_name,
                     hsize_t ncols, Value fb_value_type_id, CollectiveQueue *cq,
                     HDFIDStore *hdf_store);
  ~writer_typed_array() override = default;
  h5::append_ret write_impl(FBUF const *fbuf) override;
  uptr<h5::h5d_chunked_2d<DT>> ds;
  Value _fb_value_type_id = Value::NONE;
};

template <typename DT, typename FV>
class writer_typed_scalar : public writer_typed_base {
public:
  writer_typed_scalar(hdf5::node::Group hdf_group,
                      std::string const &source_name, Value fb_value_type_id,
                      CollectiveQueue *cq);
  writer_typed_scalar(hdf5::node::Group hdf_group,
                      std::string const &source_name, Value fb_value_type_id,
                      CollectiveQueue *cq, HDFIDStore *hdf_store);
  ~writer_typed_scalar() override = default;
  h5::append_ret write_impl(FBUF const *fbuf) override;
  uptr<h5::h5d_chunked_1d<DT>> ds;
  Value _fb_value_type_id = Value::NONE;
};

class FlatbufferReader : public FileWriter::FlatbufferReader {
  bool verify(Msg const &msg) const override;
  std::string source_name(Msg const &msg) const override;
  uint64_t timestamp(Msg const &msg) const override;
};

class HDFWriterModule : public FileWriter::HDFWriterModule {
public:
  static FileWriter::HDFWriterModule::ptr create();
  InitResult init_hdf(hdf5::node::Group &hdf_parent,
                      std::string hdf_parent_name,
                      rapidjson::Value const *attributes,
                      CollectiveQueue *cq) override;
  void parse_config(rapidjson::Value const &config_stream,
                    rapidjson::Value const *config_module) override;
  HDFWriterModule::InitResult reopen(hdf5::node::Group hdf_file,
                                     std::string hdf_parent_name,
                                     CollectiveQueue *cq,
                                     HDFIDStore *hdf_store) override;
  void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                 int mpi_rank) override;
  WriteResult write(Msg const &msg) override;
  int32_t flush() override;
  int32_t close() override;

  uptr<writer_typed_base> impl;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_timestamp;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_cue_timestamp_zero;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_cue_index;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_seq_data;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_seq_fwd;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_ts_data;
  bool do_flush_always = false;
  bool do_writer_forwarder_internal = false;
  uint64_t total_written_bytes = 0;
  uint64_t index_at_bytes = 0;
  // set by default to a large value:
  uint64_t index_every_bytes = !0;
  uint64_t ts_max = 0;
  size_t array_size = 0;
  std::string source_name;
  std::string type;
  CollectiveQueue *cq = nullptr;
};
}
}
}
