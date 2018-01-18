#include "../../FlatbufferReader.h"
#include "../../HDFFile.h"
#include "../../HDFFile_h5.h"
#include "../../HDFWriterModule.h"
#include "../../h5.h"
#include "../../helper.h"
#include <flatbuffers/flatbuffers.h>
#include <hdf5.h>
#include <limits>

namespace FileWriter {
namespace Schemas {
namespace f142 {

#include "f142_logdata_generated.h"

using std::array;
using std::vector;
using std::string;
template <typename T> using uptr = std::unique_ptr<T>;
using FBUF = LogData;
using h5::append_ret;

class writer_typed_base {
public:
  virtual ~writer_typed_base() = default;
  virtual append_ret write_impl(FBUF const *fbuf) = 0;
};

template <typename DT, typename FV>
class writer_typed_array : public writer_typed_base {
public:
  writer_typed_array(hid_t hdf_group, std::string const &source_name,
                     hsize_t ncols, Value fb_value_type_id);
  ~writer_typed_array() override = default;
  append_ret write_impl(FBUF const *fbuf) override;
  uptr<h5::h5d_chunked_2d<DT>> ds;
  Value _fb_value_type_id = Value::NONE;
};

template <typename DT, typename FV>
class writer_typed_scalar : public writer_typed_base {
public:
  writer_typed_scalar(hid_t hdf_group, std::string const &source_name,
                      Value fb_value_type_id);
  ~writer_typed_scalar() override = default;
  append_ret write_impl(FBUF const *fbuf) override;
  uptr<h5::h5d_chunked_1d<DT>> ds;
  Value _fb_value_type_id = Value::NONE;
};

static FBUF const *get_fbuf(char *data) { return GetLogData(data); }

template <typename DT, typename FV>
writer_typed_array<DT, FV>::writer_typed_array(hid_t hdf_group,
                                               std::string const &source_name,
                                               hsize_t ncols,
                                               Value fb_value_type_id)
    : _fb_value_type_id(fb_value_type_id) {
  if (ncols <= 0) {
    LOG(Sev::Error, "can not handle number of columns ncols == {}", ncols);
    return;
  }
  LOG(Sev::Debug, "f142 init_impl  ncols: {}", ncols);
  this->ds =
      h5::h5d_chunked_2d<DT>::create(hdf_group, source_name, ncols, 64 * 1024);
  if (!this->ds) {
    LOG(Sev::Error,
        "could not create hdf dataset  source_name: {}  number of columns: {}",
        source_name, ncols);
  }
}

template <typename DT, typename FV>
append_ret writer_typed_array<DT, FV>::write_impl(FBUF const *fbuf) {
  auto vt = fbuf->value_type();
  if (vt == Value::NONE || vt != _fb_value_type_id) {
    return {-2, 0, 0};
  }
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
writer_typed_scalar<DT, FV>::writer_typed_scalar(hid_t hdf_group,
                                                 std::string const &source_name,
                                                 Value fb_value_type_id)
    : _fb_value_type_id(fb_value_type_id) {
  LOG(Sev::Debug, "f142 init_impl  scalar");
  this->ds = h5::h5d_chunked_1d<DT>::create(hdf_group, source_name, 64 * 1024);
  if (!this->ds) {
    LOG(Sev::Error, "could not create hdf dataset  source_name: {}", source_name);
  }
}

template <typename DT, typename FV>
append_ret writer_typed_scalar<DT, FV>::write_impl(FBUF const *fbuf) {
  auto vt = fbuf->value_type();
  if (vt == Value::NONE || vt != _fb_value_type_id) {
    return {-2, 0, 0};
  }
  auto v1 = (FV const *)fbuf->value();
  if (!v1) {
    return {1, 0, 0};
  }
  auto v2 = v1->value();
  if (!this->ds) {
    return {1, 0, 0};
  }
  return this->ds->append_data_1d(&v2, 1);
}

class FlatbufferReader : public FileWriter::FlatbufferReader {
  bool verify(Msg const &msg) const override;
  std::string source_name(Msg const &msg) const override;
  uint64_t timestamp(Msg const &msg) const override;
};

bool FlatbufferReader::verify(Msg const &msg) const {
  auto veri = flatbuffers::Verifier((uint8_t *)msg.data, msg.size);
  return VerifyLogDataBuffer(veri);
}

std::string FlatbufferReader::source_name(Msg const &msg) const {
  auto fbuf = get_fbuf(msg.data);
  auto s1 = fbuf->source_name();
  if (!s1) {
    LOG(Sev::Warning, "message has no source name");
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
  InitResult init_hdf(hid_t hdf_file, std::string hdf_parent_name,
                      rapidjson::Value const &config_stream,
                      rapidjson::Value const *config_module) override;
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
};

FileWriter::HDFWriterModule::ptr HDFWriterModule::create() {
  return FileWriter::HDFWriterModule::ptr(new HDFWriterModule);
}

template <typename T, typename V> using WA = writer_typed_array<T, V>;
template <typename T, typename V> using WS = writer_typed_scalar<T, V>;

writer_typed_base *impl_fac(hid_t hdf_group, size_t array_size, string type,
                            string s) {
  using R = writer_typed_base *;
  auto &hg = hdf_group;
  if (array_size == 0) {
    if (type == "int8") {
      return (R) new WS<int8_t, Byte>(hg, s, Value::Byte);
    }
    if (type == "int16") {
      return (R) new WS<int16_t, Short>(hg, s, Value::Short);
    }
    if (type == "int32") {
      return (R) new WS<int32_t, Int>(hg, s, Value::Int);
    }
    if (type == "int64") {
      return (R) new WS<int64_t, Long>(hg, s, Value::Long);
    }
    if (type == "uint8") {
      return (R) new WS<uint8_t, UByte>(hg, s, Value::UByte);
    }
    if (type == "uint16") {
      return (R) new WS<uint16_t, UShort>(hg, s, Value::UShort);
    }
    if (type == "uint32") {
      return (R) new WS<uint32_t, UInt>(hg, s, Value::UInt);
    }
    if (type == "uint64") {
      return (R) new WS<uint64_t, ULong>(hg, s, Value::ULong);
    }
    if (type == "double") {
      return (R) new WS<double, Double>(hg, s, Value::Double);
    }
    if (type == "float") {
      return (R) new WS<float, Float>(hg, s, Value::Float);
    }
  } else {
    if (type == "int8") {
      return (R) new WA<int8_t, ArrayByte>(hg, s, array_size, Value::ArrayByte);
    }
    if (type == "int16") {
      return (R) new WA<int16_t, ArrayShort>(hg, s, array_size,
                                             Value::ArrayShort);
    }
    if (type == "int32") {
      return (R) new WA<int32_t, ArrayInt>(hg, s, array_size, Value::ArrayInt);
    }
    if (type == "int64") {
      return (R) new WA<int64_t, ArrayLong>(hg, s, array_size,
                                            Value::ArrayLong);
    }
    if (type == "uint8") {
      return (R) new WA<uint8_t, ArrayUByte>(hg, s, array_size,
                                             Value::ArrayUByte);
    }
    if (type == "uint16") {
      return (R) new WA<uint16_t, ArrayUShort>(hg, s, array_size,
                                               Value::ArrayUShort);
    }
    if (type == "uint32") {
      return (R) new WA<uint32_t, ArrayUInt>(hg, s, array_size,
                                             Value::ArrayUInt);
    }
    if (type == "uint64") {
      return (R) new WA<uint64_t, ArrayULong>(hg, s, array_size,
                                              Value::ArrayULong);
    }
    if (type == "double") {
      return (R) new WA<double, ArrayDouble>(hg, s, array_size,
                                             Value::ArrayDouble);
    }
    if (type == "float") {
      return (R) new WA<float, ArrayFloat>(hg, s, array_size,
                                           Value::ArrayFloat);
    }
  }
  return (writer_typed_base *)nullptr;
}

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hid_t hdf_file, std::string hdf_parent_name,
                          rapidjson::Value const &config_stream,
                          rapidjson::Value const *config_module) {
  auto hid = H5Gopen2(hdf_file, hdf_parent_name.data(), H5P_DEFAULT);
  auto &hdf_group = hid;
  auto str = get_string(&config_stream, "source");
  if (!str) {
    return HDFWriterModule::InitResult::ERROR_INCOMPLETE_CONFIGURATION();
  }
  auto source_name = str.v;
  str = get_string(&config_stream, "type");
  if (!str) {
    return HDFWriterModule::InitResult::ERROR_INCOMPLETE_CONFIGURATION();
  }
  auto type = str.v;
  size_t array_size = 0;
  if (auto x = get_uint(&config_stream, "array_size")) {
    array_size = size_t(x.v);
  }
  LOG(Sev::Debug, "HDFWriterModule::init_hdf f142 source_name: {}  type: {}  "
         "array_size: {}",
      source_name, type, array_size);

  string s("value");

  if (auto x = get_int(&config_stream, "nexus.indices.index_every_kb")) {
    index_every_bytes = uint64_t(x.v) * 1024;
  } else if (auto x = get_int(&config_stream, "nexus.indices.index_every_mb")) {
    index_every_bytes = uint64_t(x.v) * 1024 * 1024;
  }

  impl.reset(impl_fac(hdf_group, array_size, type, s));
  if (!impl) {
    LOG(Sev::Error, "Could not create a writer implementation for value_type {}", type);
    return HDFWriterModule::InitResult::ERROR_IO();
  }

  this->ds_timestamp =
      h5::h5d_chunked_1d<uint64_t>::create(hdf_group, "time", 64 * 1024);
  this->ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::create(
      hdf_group, "cue_timestamp_zero", 64 * 1024);
  this->ds_cue_index =
      h5::h5d_chunked_1d<uint64_t>::create(hdf_group, "cue_index", 64 * 1024);
  if (!ds_timestamp || !ds_cue_timestamp_zero || !ds_cue_index) {
    impl.reset();
    return HDFWriterModule::InitResult::ERROR_IO();
  }
  if (do_writer_forwarder_internal) {
    this->ds_seq_data = h5::h5d_chunked_1d<uint64_t>::create(
        hdf_group, source_name + "__fwdinfo_seq_data", 64 * 1024);
    this->ds_seq_fwd = h5::h5d_chunked_1d<uint64_t>::create(
        hdf_group, source_name + "__fwdinfo_seq_fwd", 64 * 1024);
    this->ds_ts_data = h5::h5d_chunked_1d<uint64_t>::create(
        hdf_group, source_name + "__fwdinfo_ts_data", 64 * 1024);
    if (!ds_seq_data || !ds_seq_fwd || !ds_ts_data) {
      impl.reset();
      return HDFWriterModule::InitResult::ERROR_IO();
    }
  }
  H5Gclose(hid);
  return HDFWriterModule::InitResult::OK();
}

HDFWriterModule::WriteResult HDFWriterModule::write(Msg const &msg) {
  auto fbuf = get_fbuf(msg.data);
  if (!impl) {
    LOG(Sev::Warning, "sorry, but we were unable to initialize for this kind of messages");
    return HDFWriterModule::WriteResult::ERROR_IO();
  }
  auto wret = impl->write_impl(fbuf);
  if (!wret) {
    LOG(Sev::Error, "write failed");
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
  return HDFWriterModule::WriteResult::OK_WITH_TIMESTAMP(fbuf->timestamp());
}

int32_t HDFWriterModule::flush() { return 0; }

int32_t HDFWriterModule::close() { return 0; }

HDFWriterModuleRegistry::Registrar
    g_registrar_HDFWriterModule("f142", HDFWriterModule::create);

} // namespace f142
} // namespace Schemas
} // namespace FileWriter
