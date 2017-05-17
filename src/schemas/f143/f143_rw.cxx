#include "../../HDFFile.h"
#include "../../HDFFile_h5.h"
#include "../../SchemaRegistry.h"
#include "../../h5.h"
#include "../../helper.h"
#include "schemas/f143_structure_generated.h"
#include <limits>

namespace BrightnESS {
namespace FileWriter {
namespace Schemas {
namespace f143 {

using std::array;
using std::vector;
using std::string;
template <typename T> using uptr = std::unique_ptr<T>;
using FBUF = f143_structure::Structure;

class reader : public FBSchemaReader {
  std::unique_ptr<FBSchemaWriter> create_writer_impl() override;
  bool verify_impl(Msg msg) override;
  std::string sourcename_impl(Msg msg) override;
  uint64_t ts_impl(Msg msg) override;
};

struct append_ret {
  int status;
  uint64_t written_bytes;
  uint64_t ix0;
  operator bool() const { return status == 0; }
};

class writer : public FBSchemaWriter {
  ~writer() override;
  void init_impl(std::string const &sourcename, hid_t hdf_group,
                 Msg msg) override;
  WriteResult write_impl(Msg msg) override;
  uptr<h5::h5d_chunked_1d<uint8_t>> ds_flatbuffer;
  uptr<h5::h5d_chunked_1d<uint32_t>> ds_index;
  uptr<h5::h5d_chunked_1d<uint32_t>> ds_size;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_timestamp;
  uptr<h5::h5d_chunked_1d<uint32_t>> ds_cue_index;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_cue_timestamp_zero;
  bool do_flush_always = false;
  uint64_t total_written_bytes = 0;
  uint64_t index_at_bytes = 0;
  uint64_t index_every_bytes = std::numeric_limits<uint64_t>::max();
  uint64_t ts_max = 0;
};

std::unique_ptr<FBSchemaWriter> reader::create_writer_impl() {
  return std::unique_ptr<FBSchemaWriter>(new writer);
}

static FBUF const *get_fbuf(char const *data) {
  return f143_structure::GetStructure(data);
}

bool reader::verify_impl(Msg msg) {
  LOG(3, "buffer size: {}", msg.size);
  auto veri = flatbuffers::Verifier((uint8_t *)msg.data, msg.size);
  if (f143_structure::VerifyStructureBuffer(veri))
    return true;
  return false;
}

uint64_t reader::ts_impl(Msg msg) {
  auto fbuf = get_fbuf(msg.data);
  return fbuf->timestamp();
}

std::string reader::sourcename_impl(Msg msg) {
  auto fbuf = get_fbuf(msg.data);
  auto v = fbuf->name();
  if (!v) {
    LOG(4, "WARNING message has no source name");
    return "";
  }
  return v->str();
}

writer::~writer() {}

void writer::init_impl(std::string const &sourcename, hid_t hdf_group,
                       Msg msg) {
  LOG(7, "f143::init_impl");

  if (config_file) {
    if (auto x = get_int(config_file, "nexus.indices.index_every_kb")) {
      index_every_bytes = (int)x * 1024;
    } else if (auto x = get_int(config_file, "nexus.indices.index_every_mb")) {
      index_every_bytes = (int)x * 1024 * 1024;
    }
  }

  this->ds_flatbuffer.reset(h5::h5d_chunked_1d<uint8_t>::create(
      hdf_group, "flatbuffer", 1 * 1024 * 1024));
  this->ds_index.reset(
      h5::h5d_chunked_1d<uint32_t>::create(hdf_group, "index", 64 * 1024));
  this->ds_size.reset(
      h5::h5d_chunked_1d<uint32_t>::create(hdf_group, "size", 64 * 1024));
  this->ds_timestamp.reset(
      h5::h5d_chunked_1d<uint64_t>::create(hdf_group, "timestamp", 128 * 1024));
  this->ds_cue_index.reset(
      h5::h5d_chunked_1d<uint32_t>::create(hdf_group, "cue_index", 64 * 1024));
  this->ds_cue_timestamp_zero.reset(h5::h5d_chunked_1d<uint64_t>::create(
      hdf_group, "cue_timestamp_zero", 128 * 1024));

  if (!ds_flatbuffer || !ds_index || !ds_size || !ds_timestamp ||
      !ds_cue_index || !ds_cue_timestamp_zero) {
    ds_flatbuffer.reset();
    ds_index.reset();
    ds_size.reset();
    ds_timestamp.reset();
    ds_cue_index.reset();
    ds_cue_timestamp_zero.reset();
  }
}

WriteResult writer::write_impl(Msg msg) {
  if (!ds_flatbuffer) {
    return {-1};
  }
  auto fbuf = get_fbuf(msg.data);
  uint64_t ts = fbuf->timestamp();
  auto w1ret = this->ds_flatbuffer->append_data_1d(
      (unsigned char const *)msg.data, msg.size);
  uint32_t ix0 = w1ret.ix0;
  uint32_t wb = w1ret.written_bytes;
  this->ds_index->append_data_1d(&ix0, 1);
  this->ds_size->append_data_1d(&wb, 1);
  this->ds_timestamp->append_data_1d(&ts, 1);
  total_written_bytes += w1ret.written_bytes;
  ts_max = std::max(ts, ts_max);
  if (total_written_bytes > index_at_bytes + index_every_bytes) {
    this->ds_cue_timestamp_zero->append_data_1d(&ts_max, 1);
    this->ds_cue_index->append_data_1d(&ix0, 1);
    index_at_bytes = total_written_bytes;
  }
  if (do_flush_always) {
    auto file = hdf_file->h5file_detail().h5file();
    auto err = H5Fflush(file, H5F_SCOPE_LOCAL);
    if (err < 0) {
      LOG(4, "ERROR while flushing");
    }
  }
  return {(int64_t)ts};
}

class Info : public SchemaInfo {
public:
  FBSchemaReader::ptr create_reader() override;
};

FBSchemaReader::ptr Info::create_reader() {
  return FBSchemaReader::ptr(new reader);
}

SchemaRegistry::Registrar<Info> g_registrar(fbid_from_str("f143"));

} // namespace f143
} // namespace Schemas
} // namespace FileWriter
} // namespace BrightnESS
