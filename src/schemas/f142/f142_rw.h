#include "../../HDFWriterModule.h"
#include "../../h5.h"
#include "Common.h"
#include "WriterTypedBase.h"
#include <array>
#include <memory>
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace f142 {

class HDFWriterModule : public FileWriter::HDFWriterModule {
public:
  static FileWriter::HDFWriterModule::ptr create();
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override;
  HDFWriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                 int mpi_rank) override;
  WriteResult write(Msg const &msg) override;
  int32_t flush() override;
  int32_t close() override;

  uptr<WriterTypedBase> impl;
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
