#include "../../FlatbufferReader.h"
#include "../../HDFFile.h"
#include "../../HDFFile_h5.h"
#include "../../HDFWriterModule.h"
#include "../../SchemaRegistry.h"
#include "../../h5.h"
#include "../../helper.h"
#include "schemas/f142_logdata_generated.h"
#include <hdf5.h>
#include <limits>

namespace FileWriter {
namespace Schemas {
namespace f142 {

using std::array;
using std::vector;
using std::string;
template <typename T> using uptr = std::unique_ptr<T>;
using FBUF = LogData;
using h5::append_ret;

class reader : public FBSchemaReader {
  std::unique_ptr<FBSchemaWriter> create_writer_impl() override;
  bool verify_impl(Msg msg) override;
  std::string sourcename_impl(Msg msg) override;
  uint64_t ts_impl(Msg msg) override;
  uint64_t teamid_impl(Msg &msg) override;
};

class writer_typed_base {
public:
  virtual ~writer_typed_base();
  virtual append_ret write_impl(FBUF const *fbuf) = 0;
};

template <typename DT, typename FV>
class writer_typed_array : public writer_typed_base {
public:
  writer_typed_array(hid_t hdf_group, std::string const &sourcename, FV *fbval);
  ~writer_typed_array();
  append_ret write_impl(FBUF const *fbuf) override;
  uptr<h5::h5d_chunked_2d<DT>> ds;
};

template <typename DT, typename FV>
class writer_typed_scalar : public writer_typed_base {
public:
  writer_typed_scalar(hid_t hdf_group, std::string const &sourcename,
                      FV *fbval);
  ~writer_typed_scalar();
  append_ret write_impl(FBUF const *fbuf) override;
  uptr<h5::h5d_chunked_1d<DT>> ds;
};

class writer : public FBSchemaWriter {
  ~writer() override;
  void init_impl(std::string const &sourcename, hid_t hdf_group,
                 Msg msg) override;
  WriteResult write_impl(Msg msg) override;
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
  uint64_t index_every_bytes = std::numeric_limits<uint64_t>::max();
  uint64_t ts_max = 0;
};

static FBUF const *get_fbuf(char *data) { return GetLogData(data); }

std::unique_ptr<FBSchemaWriter> reader::create_writer_impl() {
  return std::unique_ptr<FBSchemaWriter>(new writer);
}

bool reader::verify_impl(Msg msg) {
  auto veri = flatbuffers::Verifier((uint8_t *)msg.data, msg.size);
  if (VerifyLogDataBuffer(veri)) {
    return true;
  }
  return false;
}

std::string reader::sourcename_impl(Msg msg) {
  auto fbuf = get_fbuf(msg.data);
  auto s1 = fbuf->source_name();
  if (!s1) {
    LOG(4, "WARNING message has no source name");
    return "";
  }
  return s1->str();
}

uint64_t reader::ts_impl(Msg msg) {
  auto fbuf = get_fbuf(msg.data);
  return fbuf->timestamp();
}

uint64_t reader::teamid_impl(Msg &msg) {
  auto fbuf = get_fbuf(msg.data);
  if (fbuf->fwdinfo_type() == forwarder_internal::fwdinfo_1_t) {
    return ((fwdinfo_1_t *)fbuf->fwdinfo())->teamid();
  }
  return 0;
}

writer::~writer() {}

template <typename T, typename V> using WA = writer_typed_array<T, V>;
template <typename T, typename V> using WS = writer_typed_scalar<T, V>;

void writer::init_impl(string const &sourcename, hid_t hdf_group, Msg msg) {
  // This is just a unbuffered, low-performance write.
  // Improved write on separate branch.
  // Want to gather performance data first for this baseline implementation
  // in the integration tests.
  auto fbuf = get_fbuf(msg.data);
  auto &hg = hdf_group;
  string s("value");

  // todo, unify ...
  if (config_file) {
    if (auto x = get_int(config_file, "nexus.indices.index_every_mb")) {
      index_every_bytes = (int)x * 1024 * 1024;
    }
  }

  if (config_stream) {
    if (auto x = get_int(config_stream, "nexus.indices.index_every_mb")) {
      index_every_bytes = (int)x * 1024 * 1024;
    }
  }

  auto impl_fac = [&hg, &s, &fbuf](Value x) {
    using R = writer_typed_base *;
    void const *v = fbuf->value();
    if (x == Value::Byte)
      return (R) new WS<int8_t, Byte>(hg, s, (Byte *)v);
    if (x == Value::Short)
      return (R) new WS<int16_t, Short>(hg, s, (Short *)v);
    if (x == Value::Int)
      return (R) new WS<int32_t, Int>(hg, s, (Int *)v);
    if (x == Value::Long)
      return (R) new WS<int64_t, Long>(hg, s, (Long *)v);
    if (x == Value::UByte)
      return (R) new WS<uint8_t, UByte>(hg, s, (UByte *)v);
    if (x == Value::UShort)
      return (R) new WS<uint16_t, UShort>(hg, s, (UShort *)v);
    if (x == Value::UInt)
      return (R) new WS<uint32_t, UInt>(hg, s, (UInt *)v);
    if (x == Value::ULong)
      return (R) new WS<uint64_t, ULong>(hg, s, (ULong *)v);
    if (x == Value::Double)
      return (R) new WS<double, Double>(hg, s, (Double *)v);
    if (x == Value::Float)
      return (R) new WS<float, Float>(hg, s, (Float *)v);
    if (x == Value::ArrayByte)
      return (R) new WA<int8_t, ArrayByte>(hg, s, (ArrayByte *)v);
    if (x == Value::ArrayShort)
      return (R) new WA<int16_t, ArrayShort>(hg, s, (ArrayShort *)v);
    if (x == Value::ArrayInt)
      return (R) new WA<int32_t, ArrayInt>(hg, s, (ArrayInt *)v);
    if (x == Value::ArrayLong)
      return (R) new WA<int64_t, ArrayLong>(hg, s, (ArrayLong *)v);
    if (x == Value::ArrayUByte)
      return (R) new WA<uint8_t, ArrayUByte>(hg, s, (ArrayUByte *)v);
    if (x == Value::ArrayUShort)
      return (R) new WA<uint16_t, ArrayUShort>(hg, s, (ArrayUShort *)v);
    if (x == Value::ArrayUInt)
      return (R) new WA<uint32_t, ArrayUInt>(hg, s, (ArrayUInt *)v);
    if (x == Value::ArrayULong)
      return (R) new WA<uint64_t, ArrayULong>(hg, s, (ArrayULong *)v);
    if (x == Value::ArrayDouble)
      return (R) new WA<double, ArrayDouble>(hg, s, (ArrayDouble *)v);
    if (x == Value::ArrayFloat)
      return (R) new WA<float, ArrayFloat>(hg, s, (ArrayFloat *)v);
    return (writer_typed_base *)nullptr;
  };
  impl.reset(impl_fac(fbuf->value_type()));
  if (!impl) {
    LOG(4, "Could not create a writer implementation for value_type {}",
        (int)fbuf->value_type());
  }

  this->ds_timestamp =
      h5::h5d_chunked_1d<uint64_t>::create(hdf_group, "time", 64 * 1024);
  this->ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::create(
      hdf_group, "cue_timestamp_zero", 64 * 1024);
  this->ds_cue_index =
      h5::h5d_chunked_1d<uint64_t>::create(hdf_group, "cue_index", 64 * 1024);
  if (!ds_timestamp || !ds_cue_timestamp_zero || !ds_cue_index) {
    impl.reset();
    return;
  }
  if (do_writer_forwarder_internal) {
    this->ds_seq_data = h5::h5d_chunked_1d<uint64_t>::create(
        hdf_group, sourcename + "__fwdinfo_seq_data", 64 * 1024);
    this->ds_seq_fwd = h5::h5d_chunked_1d<uint64_t>::create(
        hdf_group, sourcename + "__fwdinfo_seq_fwd", 64 * 1024);
    this->ds_ts_data = h5::h5d_chunked_1d<uint64_t>::create(
        hdf_group, sourcename + "__fwdinfo_ts_data", 64 * 1024);
    if (!ds_seq_data || !ds_seq_fwd || !ds_ts_data) {
      impl.reset();
      return;
    }
  }
}

writer_typed_base::~writer_typed_base() {}

template <typename DT, typename FV>
writer_typed_array<DT, FV>::~writer_typed_array() {}

template <typename DT, typename FV>
writer_typed_array<DT, FV>::writer_typed_array(hid_t hdf_group,
                                               std::string const &dataset_name,
                                               FV *fv) {
  hsize_t ncols = fv->value()->size();
  if (ncols <= 0) {
    LOG(4, "can not handle number of columns ncols == {}", ncols);
    return;
  }
  LOG(7, "f142 init_impl  v.size(): {}", ncols);
  this->ds =
      h5::h5d_chunked_2d<DT>::create(hdf_group, dataset_name, ncols, 64 * 1024);
}

template <typename DT, typename FV>
append_ret writer_typed_array<DT, FV>::write_impl(FBUF const *fbuf) {
  auto v1 = (FV const *)fbuf->value();
  if (!v1) {
    return {1, 0, 0};
  }
  auto v2 = v1->value();
  if (!v2) {
    return {1, 0, 0};
  }
  if (!this->ds) {
    return {1, 0, 0};
  }
  return this->ds->append_data_2d(v2->data(), v2->size());
}

template <typename DT, typename FV>
writer_typed_scalar<DT, FV>::~writer_typed_scalar() {}

template <typename DT, typename FV>
writer_typed_scalar<DT, FV>::writer_typed_scalar(
    hid_t hdf_group, std::string const &dataset_name, FV *fv) {
  LOG(7, "f142 init_impl  scalar");
  this->ds = h5::h5d_chunked_1d<DT>::create(hdf_group, dataset_name, 64 * 1024);
}

template <typename DT, typename FV>
append_ret writer_typed_scalar<DT, FV>::write_impl(FBUF const *fbuf) {
  auto v1 = (FV const *)fbuf->value();
  if (!v1) {
    return {1, 0, 0};
  }
  auto v2 = v1->value();
  return this->ds->append_data_1d(&v2, 1);
}

WriteResult writer::write_impl(Msg msg) {
  auto fbuf = get_fbuf(msg.data);
  if (!impl) {
    LOG(5, "sorry, but we were unable to initialize for this kind of messages");
    return {-1};
  }
  auto wret = impl->write_impl(fbuf);
  if (!wret) {
    LOG(5, "write failed");
  }
  total_written_bytes += wret.written_bytes;
  ts_max = std::max(fbuf->timestamp(), ts_max);
  if (total_written_bytes > index_at_bytes + index_every_bytes) {
    this->ds_cue_timestamp_zero->append_data_1d(&ts_max, 1);
    this->ds_cue_index->append_data_1d(&wret.ix0, 1);
    index_at_bytes = total_written_bytes;
  }
  {
    auto x = fbuf->timestamp();
    this->ds_timestamp->append_data_1d(&x, 1);
  }
  if (do_writer_forwarder_internal) {
    if (fbuf->fwdinfo_type() == forwarder_internal::fwdinfo_1_t) {
      auto fi = (fwdinfo_1_t *)fbuf->fwdinfo();
      {
        auto x = fi->seq_data();
        this->ds_seq_data->append_data_1d(&x, 1);
      }
      {
        auto x = fi->seq_fwd();
        this->ds_seq_fwd->append_data_1d(&x, 1);
      }
      {
        auto x = fi->ts_data();
        this->ds_ts_data->append_data_1d(&x, 1);
      }
    }
  }

  if (do_flush_always) {
    auto file = hdf_file->h5file_detail().h5file();
    auto err = H5Fflush(file, H5F_SCOPE_LOCAL);
    if (err < 0) {
      LOG(4, "error while flushing");
    }
  }
  return {(int64_t)fbuf->timestamp()};
}

class Info : public SchemaInfo {
public:
  FBSchemaReader::ptr create_reader() override;
};

FBSchemaReader::ptr Info::create_reader() {
  return FBSchemaReader::ptr(new reader);
}

SchemaRegistry::Registrar<Info> g_registrar(fbid_from_str("f142"));

class FlatbufferReader : public FileWriter::FlatbufferReader {
  bool verify(Msg const &msg) const;
  std::string sourcename(Msg const &msg) const;
  uint64_t timestamp(Msg const &msg) const;
};

bool FlatbufferReader::verify(Msg const &msg) const {
  auto veri = flatbuffers::Verifier((uint8_t *)msg.data, msg.size);
  if (VerifyLogDataBuffer(veri)) {
    return true;
  }
  return false;
}

std::string FlatbufferReader::sourcename(Msg const &msg) const {
  auto fbuf = get_fbuf(msg.data);
  auto s1 = fbuf->source_name();
  if (!s1) {
    LOG(4, "message has no source name");
    return "";
  }
  return s1->str();
}

uint64_t FlatbufferReader::timestamp(Msg const &msg) const {
  auto fbuf = get_fbuf(msg.data);
  return fbuf->timestamp();
}

FlatbufferReaderRegistry::Registrar<FlatbufferReader>
    g_registrar_FlatbufferReader(fbid_from_str("f142"));

class HDFWriterModule : public FileWriter::HDFWriterModule {
public:
  static FileWriter::HDFWriterModule::ptr create();
  InitResult init_hdf(hid_t hid, rapidjson::Value const &config_stream,
                      rapidjson::Value const &config_module);
  WriteResult write(Msg const &msg);
  int32_t flush();
  int32_t close();
};

FileWriter::HDFWriterModule::ptr HDFWriterModule::create() { return nullptr; }

HDFWriterModuleRegistry::Registrar
    g_registrar_HDFWriterModule("f142_default", HDFWriterModule::create);

} // namespace f142
} // namespace Schemas
} // namespace FileWriter
