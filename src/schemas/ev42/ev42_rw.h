#include "../../FlatbufferReader.h"
#include "../../HDFFile.h"
#include "../../HDFWriterModule.h"
#include "../../h5.h"
#include "../../helper.h"
#include "../../json.h"
#include "schemas/ev42_events_generated.h"
#include <limits>

namespace FileWriter {
namespace Schemas {
namespace ev42 {
template<typename T> using uptr = std::unique_ptr<T>;

struct append_ret {
  int status;
  uint64_t written_bytes;
  uint64_t ix0;
  operator bool() const { return status == 0; }
};

class FlatbufferReader : public FileWriter::FlatbufferReader {
  bool verify(Msg const &msg) const override;
  std::string source_name(Msg const &msg) const override;
  uint64_t timestamp(Msg const &msg) const override;
  //add in others
};

class HDFWriterModule : public FileWriter::HDFWriterModule {
public:
  static FileWriter::HDFWriterModule::ptr create();
  void parse_config(rapidjson::Value const &config_stream,
                    rapidjson::Value const *config_module) override;
  InitResult init_hdf(hdf5::node::Group &hdf_parent,
                      std::string hdf_parent_name,
                      rapidjson::Value const *attributes,
                      CollectiveQueue *cq) override;
  InitResult reopen(hid_t hdf_file, std::string hdf_parent_name, CollectiveQueue *cq,
                    HDFIDStore *hdf_store) override;
  WriteResult write(Msg const &msg) override;
  int32_t flush() override;
  int32_t close() override;
  void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                 int mpi_rank) override;

  uptr<h5::h5d_chunked_1d<uint32_t>> ds_event_time_offset;
  uptr<h5::h5d_chunked_1d<uint32_t>> ds_event_id;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_event_time_zero;
  uptr<h5::h5d_chunked_1d<uint32_t>> ds_event_index;
  uptr<h5::h5d_chunked_1d<uint32_t>> ds_cue_index;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_cue_timestamp_zero;
  hsize_t chunk_bytes = 1 << 16;
  bool do_flush_always = false;
  uint64_t total_written_bytes = 0;
  uint64_t index_at_bytes = 0;
  uint64_t index_every_bytes = std::numeric_limits<uint64_t>::max();
  uint64_t ts_max = 0;
  size_t buffer_size = 0;
  size_t buffer_packet_max = 0;
  CollectiveQueue *cq = nullptr;
};
}
}
}
