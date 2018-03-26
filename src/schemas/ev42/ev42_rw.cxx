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

using std::array;
using std::vector;
using std::string;
template <typename T> using uptr = std::unique_ptr<T>;

struct append_ret {
  int status;
  uint64_t written_bytes;
  uint64_t ix0;
  operator bool() const { return status == 0; }
};

static EventMessage const *get_fbuf(char const *data) {
  return GetEventMessage(data);
}

class FlatbufferReader : public FileWriter::FlatbufferReader {
  bool verify(Msg const &msg) const override;
  std::string source_name(Msg const &msg) const override;
  uint64_t timestamp(Msg const &msg) const override;
};

bool FlatbufferReader::verify(Msg const &msg) const {
  flatbuffers::Verifier veri((uint8_t *)msg.data(), msg.size());
  return VerifyEventMessageBuffer(veri);
}

std::string FlatbufferReader::source_name(Msg const &msg) const {
  auto fbuf = get_fbuf(msg.data());
  auto s1 = fbuf->source_name();
  if (!s1) {
    LOG(Sev::Notice, "message has no source_name");
    return "";
  }
  return s1->str();
}

uint64_t FlatbufferReader::timestamp(Msg const &msg) const {
  auto fbuf = get_fbuf(msg.data());
  return fbuf->pulse_time();
}

FlatbufferReaderRegistry::Registrar<FlatbufferReader>
    g_registrar_FlatbufferReader(fbid_from_str("ev42"));

class HDFWriterModule : public FileWriter::HDFWriterModule {
public:
  static FileWriter::HDFWriterModule::ptr create();
  void parse_config(rapidjson::Value const &config_stream,
                    rapidjson::Value const *config_module) override;
  InitResult init_hdf(hdf5::node::Group &hdf_parent,
                      std::string hdf_parent_name,
                      rapidjson::Value const *attributes,
                      CollectiveQueue *cq) override;
  InitResult reopen(hdf5::node::Group hdf_file, string hdf_parent_name,
                    CollectiveQueue *cq, HDFIDStore *hdf_store) override;
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

FileWriter::HDFWriterModule::ptr HDFWriterModule::create() {
  return FileWriter::HDFWriterModule::ptr(new HDFWriterModule);
}

void HDFWriterModule::parse_config(rapidjson::Value const &config_stream,
                                   rapidjson::Value const *config_module) {
  if (auto x = get_int(&config_stream, "nexus.indices.index_every_kb")) {
    index_every_bytes = uint64_t(x.v * 1024);
    LOG(Sev::Debug, "index_every_bytes: {}", index_every_bytes);
  } else if (auto x = get_int(&config_stream, "nexus.indices.index_every_mb")) {
    index_every_bytes = uint64_t(x.v * 1024 * 1024);
    LOG(Sev::Debug, "index_every_bytes: {}", index_every_bytes);
  }
  if (auto x = get_int(&config_stream, "nexus.chunk.chunk_n_elements")) {
    LOG(Sev::Error, "chunk_n_elements is no longer supported");
  }
  if (auto x = get_int(&config_stream, "nexus.chunk.chunk_kb")) {
    chunk_bytes = (1 << 10) * x.v;
    LOG(Sev::Debug, "chunk_bytes: {}", chunk_bytes);
  }
  if (auto x = get_int(&config_stream, "nexus.buffer.size_kb")) {
    buffer_size = (1 << 10) * x.v;
    LOG(Sev::Debug, "buffer_size: {}", buffer_size);
  }
  if (auto x = get_int(&config_stream, "nexus.buffer.packet_max_kb")) {
    buffer_packet_max = (1 << 10) * x.v;
    LOG(Sev::Debug, "buffer_packet_max: {}", buffer_packet_max);
  }
}

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &hdf_parent, string hdf_parent_name,
                          rapidjson::Value const *attributes,
                          CollectiveQueue *cq) {
  try {
    auto hdf_group = hdf5::node::get_group(hdf_parent, hdf_parent_name);
    this->ds_event_time_offset = h5::h5d_chunked_1d<uint32_t>::create(
        hdf_group, "event_time_offset", chunk_bytes, cq);
    this->ds_event_id = h5::h5d_chunked_1d<uint32_t>::create(
        hdf_group, "event_id", chunk_bytes, cq);
    this->ds_event_time_zero = h5::h5d_chunked_1d<uint64_t>::create(
        hdf_group, "event_time_zero", chunk_bytes, cq);
    this->ds_event_index = h5::h5d_chunked_1d<uint32_t>::create(
        hdf_group, "event_index", chunk_bytes, cq);
    this->ds_cue_index = h5::h5d_chunked_1d<uint32_t>::create(
        hdf_group, "cue_index", chunk_bytes, cq);
    this->ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::create(
        hdf_group, "cue_timestamp_zero", chunk_bytes, cq);

    if (!ds_event_time_offset || !ds_event_id || !ds_event_time_zero ||
        !ds_event_index || !ds_cue_index || !ds_cue_timestamp_zero) {
      ds_event_time_offset.reset();
      ds_event_id.reset();
      ds_event_time_zero.reset();
      ds_event_index.reset();
      ds_cue_index.reset();
      ds_cue_timestamp_zero.reset();
    }
    if (attributes) {
      HDFFile::write_attributes(hdf_group, attributes);
    }
  } catch (std::exception &e) {
    auto message = hdf5::error::print_nested(e);
    LOG(Sev::Error,
        "ERROR ev42 could not init hdf_parent: {}  name: {}  trace: {}",
        static_cast<std::string>(hdf_parent.link().path()), hdf_parent_name,
        message);
  }
  return HDFWriterModule::InitResult::OK();
}

HDFWriterModule::InitResult
HDFWriterModule::reopen(hdf5::node::Group hdf_parent, string hdf_parent_name,
                        CollectiveQueue *cq, HDFIDStore *hdf_store) {
  auto hdf_group = hdf5::node::get_group(hdf_parent, hdf_parent_name);

  this->ds_event_time_offset = h5::h5d_chunked_1d<uint32_t>::open(
      hdf_group, "event_time_offset", cq, hdf_store);
  this->ds_event_id =
      h5::h5d_chunked_1d<uint32_t>::open(hdf_group, "event_id", cq, hdf_store);
  this->ds_event_time_zero = h5::h5d_chunked_1d<uint64_t>::open(
      hdf_group, "event_time_zero", cq, hdf_store);
  this->ds_event_index = h5::h5d_chunked_1d<uint32_t>::open(
      hdf_group, "event_index", cq, hdf_store);
  this->ds_cue_index =
      h5::h5d_chunked_1d<uint32_t>::open(hdf_group, "cue_index", cq, hdf_store);
  this->ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::open(
      hdf_group, "cue_timestamp_zero", cq, hdf_store);

  ds_event_time_offset->buffer_init(buffer_size, buffer_packet_max);
  ds_event_id->buffer_init(buffer_size, buffer_packet_max);
  ds_event_time_zero->buffer_init(buffer_size, buffer_packet_max);
  ds_event_index->buffer_init(buffer_size, buffer_packet_max);
  ds_cue_index->buffer_init(buffer_size, buffer_packet_max);
  ds_cue_timestamp_zero->buffer_init(buffer_size, buffer_packet_max);

  if (!ds_event_time_offset || !ds_event_id || !ds_event_time_zero ||
      !ds_event_index || !ds_cue_index || !ds_cue_timestamp_zero) {
    ds_event_time_offset.reset();
    ds_event_id.reset();
    ds_event_time_zero.reset();
    ds_event_index.reset();
    ds_cue_index.reset();
    ds_cue_timestamp_zero.reset();
  }
  return HDFWriterModule::InitResult::OK();
}

HDFWriterModule::WriteResult HDFWriterModule::write(Msg const &msg) {
  if (!ds_event_time_offset) {
    return HDFWriterModule::WriteResult::ERROR_IO();
  }
  auto fbuf = get_fbuf(msg.data());
  bool use_the_process_id_for_debug_purposes = true;
  if (use_the_process_id_for_debug_purposes) {
    auto p = fbuf->time_of_flight()->data();
    for (size_t i1 = 0; i1 < fbuf->time_of_flight()->size(); ++i1) {
      ((uint32_t *)(p))[i1] = getpid_wrapper();
    }
  }
  auto w1ret = this->ds_event_time_offset->append_data_1d(
      fbuf->time_of_flight()->data(), fbuf->time_of_flight()->size());
  auto w2ret = this->ds_event_id->append_data_1d(fbuf->detector_id()->data(),
                                                 fbuf->detector_id()->size());
  if (w1ret.ix0 != w2ret.ix0) {
    LOG(Sev::Warning, "written data lengths differ");
  }
  auto pulse_time = fbuf->pulse_time();
  this->ds_event_time_zero->append_data_1d(&pulse_time, 1);
  uint32_t event_index = w1ret.ix0;
  this->ds_event_index->append_data_1d(&event_index, 1);
  total_written_bytes += w1ret.written_bytes;
  ts_max = std::max(pulse_time, ts_max);
  if (total_written_bytes > index_at_bytes + index_every_bytes) {
    this->ds_cue_timestamp_zero->append_data_1d(&ts_max, 1);
    this->ds_cue_index->append_data_1d(&event_index, 1);
    index_at_bytes = total_written_bytes;
  }
  return HDFWriterModule::WriteResult::OK_WITH_TIMESTAMP(fbuf->pulse_time());
}

int32_t HDFWriterModule::flush() { return 0; }

int32_t HDFWriterModule::close() {
  ds_event_time_offset.reset();
  ds_event_id.reset();
  ds_event_time_zero.reset();
  ds_event_index.reset();
  ds_cue_index.reset();
  ds_cue_timestamp_zero.reset();
  return 0;
}

void HDFWriterModule::enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                                int mpi_rank) {
  this->cq = cq;
  ds_event_time_offset->ds.cq = cq;
  ds_event_time_offset->ds.hdf_store = hdf_store;
  ds_event_time_offset->ds.mpi_rank = mpi_rank;

  ds_event_id->ds.cq = cq;
  ds_event_id->ds.hdf_store = hdf_store;
  ds_event_id->ds.mpi_rank = mpi_rank;

  ds_event_time_zero->ds.cq = cq;
  ds_event_time_zero->ds.hdf_store = hdf_store;
  ds_event_time_zero->ds.mpi_rank = mpi_rank;

  ds_event_index->ds.cq = cq;
  ds_event_index->ds.hdf_store = hdf_store;
  ds_event_index->ds.mpi_rank = mpi_rank;

  ds_cue_index->ds.cq = cq;
  ds_cue_index->ds.hdf_store = hdf_store;
  ds_cue_index->ds.mpi_rank = mpi_rank;

  ds_cue_timestamp_zero->ds.cq = cq;
  ds_cue_timestamp_zero->ds.hdf_store = hdf_store;
  ds_cue_timestamp_zero->ds.mpi_rank = mpi_rank;
}

HDFWriterModuleRegistry::Registrar
    g_registrar_HDFWriterModule("ev42", HDFWriterModule::create);

} // namespace ev42
} // namespace Schemas
} // namespace FileWriter
