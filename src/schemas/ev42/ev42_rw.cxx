#include "../../FlatbufferReader.h"
#include "../../HDFFile.h"
#include "../../HDFWriterModule.h"
#include "../../h5.h"
#include "../../helper.h"
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
  std::string sourcename(Msg const &msg) const override;
  uint64_t timestamp(Msg const &msg) const override;
};

bool FlatbufferReader::verify(Msg const &msg) const {
  flatbuffers::Verifier veri((uint8_t *)msg.data(), msg.size());
  return VerifyEventMessageBuffer(veri);
}

std::string FlatbufferReader::sourcename(Msg const &msg) const {
  auto fbuf = get_fbuf(msg.data());
  auto s1 = fbuf->source_name();
  if (!s1) {
    LOG(4, "message has no source name");
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
  InitResult init_hdf(hid_t hdf_file, string hdf_parent_name,
                      CollectiveQueue *cq) override;
  InitResult reopen(hid_t hdf_file, string hdf_parent_name, CollectiveQueue *cq,
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
  hsize_t chunk_n_elements = 1000000;
  bool do_flush_always = false;
  uint64_t total_written_bytes = 0;
  uint64_t index_at_bytes = 0;
  uint64_t index_every_bytes = std::numeric_limits<uint64_t>::max();
  uint64_t ts_max = 0;
  CollectiveQueue *cq = nullptr;
};

FileWriter::HDFWriterModule::ptr HDFWriterModule::create() {
  return FileWriter::HDFWriterModule::ptr(new HDFWriterModule);
}

void HDFWriterModule::parse_config(rapidjson::Value const &config_stream,
                                   rapidjson::Value const *config_module) {
  if (auto x = get_int(&config_stream, "nexus.indices.index_every_kb")) {
    index_every_bytes = uint64_t(x.v * 1024);
    LOG(7, "index_every_bytes: {}", index_every_bytes);
  } else if (auto x = get_int(&config_stream, "nexus.indices.index_every_mb")) {
    index_every_bytes = uint64_t(x.v * 1024 * 1024);
    LOG(7, "index_every_bytes: {}", index_every_bytes);
  }
  if (auto x = get_int(&config_stream, "nexus.chunk.chunk_n_elements")) {
    chunk_n_elements = hsize_t(x.v);
    LOG(7, "chunk_n_elements: {}", chunk_n_elements);
  }
}

HDFWriterModule::InitResult HDFWriterModule::init_hdf(hid_t hdf_file,
                                                      string hdf_parent_name,
                                                      CollectiveQueue *cq) {
  auto hid = H5Gopen2(hdf_file, hdf_parent_name.data(), H5P_DEFAULT);

  this->ds_event_time_offset = h5::h5d_chunked_1d<uint32_t>::create(
      hid, "event_time_offset", chunk_n_elements * sizeof(uint32_t), cq);
  this->ds_event_id = h5::h5d_chunked_1d<uint32_t>::create(
      hid, "event_id", chunk_n_elements * sizeof(uint32_t), cq);
  this->ds_event_time_zero = h5::h5d_chunked_1d<uint64_t>::create(
      hid, "event_time_zero", chunk_n_elements * sizeof(uint64_t), cq);
  this->ds_event_index = h5::h5d_chunked_1d<uint32_t>::create(
      hid, "event_index", chunk_n_elements * sizeof(uint32_t), cq);
  this->ds_cue_index = h5::h5d_chunked_1d<uint32_t>::create(
      hid, "cue_index", chunk_n_elements * sizeof(uint32_t), cq);
  this->ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::create(
      hid, "cue_timestamp_zero", chunk_n_elements * sizeof(uint64_t), cq);

  if (!ds_event_time_offset || !ds_event_id || !ds_event_time_zero ||
      !ds_event_index || !ds_cue_index || !ds_cue_timestamp_zero) {
    ds_event_time_offset.reset();
    ds_event_id.reset();
    ds_event_time_zero.reset();
    ds_event_index.reset();
    ds_cue_index.reset();
    ds_cue_timestamp_zero.reset();
  }
  H5Gclose(hid);
  return HDFWriterModule::InitResult::OK();
}

HDFWriterModule::InitResult HDFWriterModule::reopen(hid_t hdf_file,
                                                    string hdf_parent_name,
                                                    CollectiveQueue *cq,
                                                    HDFIDStore *hdf_store) {
  auto hid = H5Gopen2(hdf_file, hdf_parent_name.data(), H5P_DEFAULT);

  this->ds_event_time_offset = h5::h5d_chunked_1d<uint32_t>::open(
      hid, "event_time_offset", cq, hdf_store);
  this->ds_event_id =
      h5::h5d_chunked_1d<uint32_t>::open(hid, "event_id", cq, hdf_store);
  this->ds_event_time_zero =
      h5::h5d_chunked_1d<uint64_t>::open(hid, "event_time_zero", cq, hdf_store);
  this->ds_event_index =
      h5::h5d_chunked_1d<uint32_t>::open(hid, "event_index", cq, hdf_store);
  this->ds_cue_index =
      h5::h5d_chunked_1d<uint32_t>::open(hid, "cue_index", cq, hdf_store);
  this->ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::open(
      hid, "cue_timestamp_zero", cq, hdf_store);

  if (!ds_event_time_offset || !ds_event_id || !ds_event_time_zero ||
      !ds_event_index || !ds_cue_index || !ds_cue_timestamp_zero) {
    ds_event_time_offset.reset();
    ds_event_id.reset();
    ds_event_time_zero.reset();
    ds_event_index.reset();
    ds_cue_index.reset();
    ds_cue_timestamp_zero.reset();
  }
  H5Gclose(hid);
  return HDFWriterModule::InitResult::OK();
}

HDFWriterModule::WriteResult HDFWriterModule::write(Msg const &msg) {
  if (!ds_event_time_offset) {
    return HDFWriterModule::WriteResult::ERROR_IO();
  }
  auto fbuf = get_fbuf(msg.data());
  // int64_t ts = fbuf->pulse_time();
  auto w1ret = this->ds_event_time_offset->append_data_1d(
      fbuf->time_of_flight()->data(), fbuf->time_of_flight()->size());
  auto w2ret = this->ds_event_id->append_data_1d(fbuf->detector_id()->data(),
                                                 fbuf->detector_id()->size());
  if (w1ret.ix0 != w2ret.ix0) {
    LOG(3, "written data lengths differ");
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
