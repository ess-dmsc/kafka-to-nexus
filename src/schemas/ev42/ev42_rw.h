#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include "../../h5.h"

namespace FileWriter {
namespace Schemas {
namespace ev42 {
template <typename T> using uptr = std::unique_ptr<T>;

class FlatbufferReader : public FileWriter::FlatbufferReader {
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;
  // add in others
};

class HDFWriterModule : public FileWriter::HDFWriterModule {
public:
  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override;
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  HDFWriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;
  WriteResult write(FlatbufferMessage const &Message) override;
  int32_t flush() override;
  int32_t close() override;

  uptr<h5::h5d_chunked_1d<uint32_t>> ds_event_time_offset;
  uptr<h5::h5d_chunked_1d<uint32_t>> ds_event_id;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_event_time_zero;
  uptr<h5::h5d_chunked_1d<uint32_t>> ds_event_index;
  uptr<h5::h5d_chunked_1d<uint32_t>> ds_cue_index;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_cue_timestamp_zero;
  hsize_t chunk_bytes = 1 << 16;
  uint64_t total_written_bytes = 0;
  uint64_t index_at_bytes = 0;
  uint64_t index_every_bytes = std::numeric_limits<uint64_t>::max();
  uint64_t ts_max = 0;
  size_t buffer_size = 0;
  size_t buffer_packet_max = 0;
};
} // namespace ev42
} // namespace Schemas
} // namespace FileWriter
