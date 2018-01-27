#include "../../CollectiveQueue.h"
#include "../../FlatbufferReader.h"
#include "../../HDFFile.h"
#include "../../HDFWriterModule.h"
#include "../../h5.h"
#include "../../helper.h"
#include "../../json.h"
#include <flatbuffers/flatbuffers.h>
#include <hdf5.h>
#include <limits>

namespace FileWriter {
namespace Schemas {
namespace f142 {

#include "schemas/f142_logdata_generated.h"

using std::array;
using std::vector;
using std::string;
template <typename T> using uptr = std::unique_ptr<T>;
using FBUF = LogData;

class writer_typed_base {
public:
  virtual ~writer_typed_base() = default;
  virtual h5::append_ret write_impl(FBUF const *fbuf) = 0;
};

template <typename DT, typename FV>
class writer_typed_array : public writer_typed_base {
public:
  writer_typed_array(hid_t hdf_group, std::string const &source_name,
                     hsize_t ncols, Value fb_value_type_id,
                     CollectiveQueue *cq);
  writer_typed_array(hid_t hdf_group, std::string const &source_name,
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
  writer_typed_scalar(hid_t hdf_group, std::string const &source_name,
                      Value fb_value_type_id, CollectiveQueue *cq);
  writer_typed_scalar(hid_t hdf_group, std::string const &source_name,
                      Value fb_value_type_id, CollectiveQueue *cq,
                      HDFIDStore *hdf_store);
  ~writer_typed_scalar() override = default;
  h5::append_ret write_impl(FBUF const *fbuf) override;
  uptr<h5::h5d_chunked_1d<DT>> ds;
  Value _fb_value_type_id = Value::NONE;
};

static FBUF const *get_fbuf(char const *data) { return GetLogData(data); }

template <typename DT, typename FV>
writer_typed_array<DT, FV>::writer_typed_array(hid_t hdf_group,
                                               std::string const &source_name,
                                               hsize_t ncols,
                                               Value fb_value_type_id,
                                               CollectiveQueue *cq)
    : _fb_value_type_id(fb_value_type_id) {
  if (ncols <= 0) {
    LOG(Sev::Error, "can not handle number of columns ncols == {}", ncols);
    return;
  }
  LOG(Sev::Debug, "f142 init_impl  ncols: {}", ncols);
  this->ds = h5::h5d_chunked_2d<DT>::create(hdf_group, source_name, ncols,
                                            64 * 1024, cq);
  if (!this->ds) {
    LOG(Sev::Error,
        "could not create hdf dataset  source_name: {}  number of columns: {}",
        source_name, ncols);
  }
}

template <typename DT, typename FV>
writer_typed_array<DT, FV>::writer_typed_array(
    hid_t hdf_group, std::string const &source_name, hsize_t ncols,
    Value fb_value_type_id, CollectiveQueue *cq, HDFIDStore *hdf_store)
    : _fb_value_type_id(fb_value_type_id) {
  if (ncols <= 0) {
    LOG(Sev::Error, "can not handle number of columns ncols == {}", ncols);
    return;
  }
  LOG(Sev::Debug, "f142 writer_typed_array reopen  ncols: {}", ncols);
  ds = h5::h5d_chunked_2d<DT>::open(hdf_group, source_name, ncols, cq,
                                    hdf_store);
  if (!ds) {
    LOG(Sev::Error,
        "could not create hdf dataset  source_name: {}  number of columns: {}",
        source_name, ncols);
  }
  // TODO take from config
  ds->buffer_init(64 * 1024, 0);
}

template <typename DT, typename FV>
h5::append_ret writer_typed_array<DT, FV>::write_impl(FBUF const *fbuf) {
  auto vt = fbuf->value_type();
  if (vt == Value::NONE || vt != _fb_value_type_id) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v1 = (FV const *)fbuf->value();
  if (!v1) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v2 = v1->value();
  if (!v2) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  if (!this->ds) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  return this->ds->append_data_2d(v2->data(), v2->size());
}

template <typename DT, typename FV>
writer_typed_scalar<DT, FV>::writer_typed_scalar(hid_t hdf_group,
                                                 std::string const &source_name,
                                                 Value fb_value_type_id,
                                                 CollectiveQueue *cq)
    : _fb_value_type_id(fb_value_type_id) {
  LOG(Sev::Debug, "f142 init_impl  scalar");
  this->ds =
      h5::h5d_chunked_1d<DT>::create(hdf_group, source_name, 64 * 1024, cq);
  if (!this->ds) {
    LOG(Sev::Error, "could not create hdf dataset  source_name: {}",
        source_name);
  }
}

template <typename DT, typename FV>
writer_typed_scalar<DT, FV>::writer_typed_scalar(hid_t hdf_group,
                                                 std::string const &source_name,
                                                 Value fb_value_type_id,
                                                 CollectiveQueue *cq,
                                                 HDFIDStore *hdf_store)
    : _fb_value_type_id(fb_value_type_id) {
  LOG(Sev::Debug, "f142 init_impl  scalar");
  ds = h5::h5d_chunked_1d<DT>::open(hdf_group, source_name, cq, hdf_store);
  if (!this->ds) {
    LOG(Sev::Error, "could not create hdf dataset  source_name: {}",
        source_name);
  }
  // TODO take from config
  ds->buffer_init(64 * 1024, 0);
}

template <typename DT, typename FV>
h5::append_ret writer_typed_scalar<DT, FV>::write_impl(FBUF const *fbuf) {
  auto vt = fbuf->value_type();
  if (vt == Value::NONE || vt != _fb_value_type_id) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v1 = (FV const *)fbuf->value();
  if (!v1) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v2 = v1->value();
  if (!this->ds) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  return this->ds->append_data_1d(&v2, 1);
}

class FlatbufferReader : public FileWriter::FlatbufferReader {
  bool verify(Msg const &msg) const override;
  std::string source_name(Msg const &msg) const override;
  uint64_t timestamp(Msg const &msg) const override;
};

bool FlatbufferReader::verify(Msg const &msg) const {
  auto veri = flatbuffers::Verifier((uint8_t *)msg.data(), msg.size());
  return VerifyLogDataBuffer(veri);
}

std::string FlatbufferReader::source_name(Msg const &msg) const {
  auto fbuf = get_fbuf(msg.data());
  auto s1 = fbuf->source_name();
  if (!s1) {
    LOG(Sev::Warning, "message has no source name");
    return "";
  }
  return s1->str();
}

uint64_t FlatbufferReader::timestamp(Msg const &msg) const {
  auto fbuf = get_fbuf(msg.data());
  return fbuf->timestamp();
}

FlatbufferReaderRegistry::Registrar<FlatbufferReader>
    g_registrar_FlatbufferReader(fbid_from_str("f142"));

class HDFWriterModule : public FileWriter::HDFWriterModule {
public:
  static FileWriter::HDFWriterModule::ptr create();
  InitResult init_hdf(hdf5::node::Group& hdf_parent,
                      std::string hdf_parent_name,
                      rapidjson::Value const *attributes,
                      CollectiveQueue *cq) override;
  void parse_config(rapidjson::Value const &config_stream,
                    rapidjson::Value const *config_module) override;
  InitResult reopen(hid_t hdf_file, std::string hdf_parent_name,
                    CollectiveQueue *cq, HDFIDStore *hdf_store) override;
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

FileWriter::HDFWriterModule::ptr HDFWriterModule::create() {
  return FileWriter::HDFWriterModule::ptr(new HDFWriterModule);
}

template <typename T, typename V> using WA = writer_typed_array<T, V>;
template <typename T, typename V> using WS = writer_typed_scalar<T, V>;

static FileWriter::Schemas::f142::Value
value_type_scalar_from_string(string type) {
  if (type == "int8") {
    return Value::Byte;
  }
  if (type == "int16") {
    return Value::Short;
  }
  if (type == "int32") {
    return Value::Int;
  }
  if (type == "int64") {
    return Value::Long;
  }
  if (type == "uint8") {
    return Value::UByte;
  }
  if (type == "uint16") {
    return Value::UShort;
  }
  if (type == "uint32") {
    return Value::UInt;
  }
  if (type == "uint64") {
    return Value::ULong;
  }
  if (type == "float") {
    return Value::Float;
  }
  if (type == "double") {
    return Value::Double;
  }
  return Value::Int;
}

static FileWriter::Schemas::f142::Value
value_type_array_from_string(string type) {
  if (type == "int8") {
    return Value::ArrayByte;
  }
  if (type == "int16") {
    return Value::ArrayShort;
  }
  if (type == "int32") {
    return Value::ArrayInt;
  }
  if (type == "int64") {
    return Value::ArrayLong;
  }
  if (type == "uint8") {
    return Value::ArrayUByte;
  }
  if (type == "uint16") {
    return Value::ArrayUShort;
  }
  if (type == "uint32") {
    return Value::ArrayUInt;
  }
  if (type == "uint64") {
    return Value::ArrayULong;
  }
  if (type == "float") {
    return Value::ArrayFloat;
  }
  if (type == "double") {
    return Value::ArrayDouble;
  }
  return Value::Int;
}

// clang-format: off

// should make this templated..

writer_typed_base *impl_fac(hid_t hdf_group, size_t array_size, string type,
                            string s, CollectiveQueue *cq) {
  using R = writer_typed_base *;
  auto &hg = hdf_group;
  if (array_size == 0) {
    auto vt = value_type_scalar_from_string(type);
    if (type == "int8") {
      return (R) new WS<int8_t, Byte>(hg, s, vt, cq);
    }
    if (type == "int16") {
      return (R) new WS<int16_t, Short>(hg, s, vt, cq);
    }
    if (type == "int32") {
      return (R) new WS<int32_t, Int>(hg, s, vt, cq);
    }
    if (type == "int64") {
      return (R) new WS<int64_t, Long>(hg, s, vt, cq);
    }
    if (type == "uint8") {
      return (R) new WS<uint8_t, UByte>(hg, s, vt, cq);
    }
    if (type == "uint16") {
      return (R) new WS<uint16_t, UShort>(hg, s, vt, cq);
    }
    if (type == "uint32") {
      return (R) new WS<uint32_t, UInt>(hg, s, vt, cq);
    }
    if (type == "uint64") {
      return (R) new WS<uint64_t, ULong>(hg, s, vt, cq);
    }
    if (type == "float") {
      return (R) new WS<float, Float>(hg, s, vt, cq);
    }
    if (type == "double") {
      return (R) new WS<double, Double>(hg, s, vt, cq);
    }
  } else {
    auto vt = value_type_array_from_string(type);
    if (type == "int8") {
      return (R) new WA<int8_t, ArrayByte>(hg, s, array_size, vt, cq);
    }
    if (type == "int16") {
      return (R) new WA<int16_t, ArrayShort>(hg, s, array_size, vt, cq);
    }
    if (type == "int32") {
      return (R) new WA<int32_t, ArrayInt>(hg, s, array_size, vt, cq);
    }
    if (type == "int64") {
      return (R) new WA<int64_t, ArrayLong>(hg, s, array_size, vt, cq);
    }
    if (type == "uint8") {
      return (R) new WA<uint8_t, ArrayUByte>(hg, s, array_size, vt, cq);
    }
    if (type == "uint16") {
      return (R) new WA<uint16_t, ArrayUShort>(hg, s, array_size, vt, cq);
    }
    if (type == "uint32") {
      return (R) new WA<uint32_t, ArrayUInt>(hg, s, array_size, vt, cq);
    }
    if (type == "uint64") {
      return (R) new WA<uint64_t, ArrayULong>(hg, s, array_size, vt, cq);
    }
    if (type == "float") {
      return (R) new WA<float, ArrayFloat>(hg, s, array_size, vt, cq);
    }
    if (type == "double") {
      return (R) new WA<double, ArrayDouble>(hg, s, array_size, vt, cq);
    }
  }
  return (writer_typed_base *)nullptr;
}

writer_typed_base *impl_fac_open(hid_t hdf_group, size_t array_size,
                                 string type, string s, CollectiveQueue *cq,
                                 HDFIDStore *hdf_store) {
  using R = writer_typed_base *;
  auto &hg = hdf_group;
  if (array_size == 0) {
    if (type == "int8") {
      return (R) new WS<int8_t, Byte>(hg, s, Value::Byte, cq, hdf_store);
    }
    if (type == "int16") {
      return (R) new WS<int16_t, Short>(hg, s, Value::Short, cq, hdf_store);
    }
    if (type == "int32") {
      return (R) new WS<int32_t, Int>(hg, s, Value::Int, cq, hdf_store);
    }
    if (type == "int64") {
      return (R) new WS<int64_t, Long>(hg, s, Value::Long, cq, hdf_store);
    }
    if (type == "uint8") {
      return (R) new WS<uint8_t, UByte>(hg, s, Value::UByte, cq, hdf_store);
    }
    if (type == "uint16") {
      return (R) new WS<uint16_t, UShort>(hg, s, Value::UShort, cq, hdf_store);
    }
    if (type == "uint32") {
      return (R) new WS<uint32_t, UInt>(hg, s, Value::UInt, cq, hdf_store);
    }
    if (type == "uint64") {
      return (R) new WS<uint64_t, ULong>(hg, s, Value::ULong, cq, hdf_store);
    }
    if (type == "float") {
      return (R) new WS<float, Float>(hg, s, Value::Float, cq, hdf_store);
    }
    if (type == "double") {
      return (R) new WS<double, Double>(hg, s, Value::Double, cq, hdf_store);
    }
  } else {
    if (type == "int8") {
      return (R) new WA<int8_t, ArrayByte>(hg, s, array_size, Value::ArrayByte,
                                           cq, hdf_store);
    }
    if (type == "int16") {
      return (R) new WA<int16_t, ArrayShort>(hg, s, array_size,
                                             Value::ArrayShort, cq, hdf_store);
    }
    if (type == "int32") {
      return (R) new WA<int32_t, ArrayInt>(hg, s, array_size, Value::ArrayInt,
                                           cq, hdf_store);
    }
    if (type == "int64") {
      return (R) new WA<int64_t, ArrayLong>(hg, s, array_size, Value::ArrayLong,
                                            cq, hdf_store);
    }
    if (type == "uint8") {
      return (R) new WA<uint8_t, ArrayUByte>(hg, s, array_size,
                                             Value::ArrayUByte, cq, hdf_store);
    }
    if (type == "uint16") {
      return (R) new WA<uint16_t, ArrayUShort>(
          hg, s, array_size, Value::ArrayUShort, cq, hdf_store);
    }
    if (type == "uint32") {
      return (R) new WA<uint32_t, ArrayUInt>(hg, s, array_size,
                                             Value::ArrayUInt, cq, hdf_store);
    }
    if (type == "uint64") {
      return (R) new WA<uint64_t, ArrayULong>(hg, s, array_size,
                                              Value::ArrayULong, cq, hdf_store);
    }
    if (type == "float") {
      return (R) new WA<float, ArrayFloat>(hg, s, array_size, Value::ArrayFloat,
                                           cq, hdf_store);
    }
    if (type == "double") {
      return (R) new WA<double, ArrayDouble>(hg, s, array_size,
                                             Value::ArrayDouble, cq, hdf_store);
    }
  }
  return (writer_typed_base *)nullptr;
}

// clang-format: on

void HDFWriterModule::parse_config(rapidjson::Value const &config_stream,
                                   rapidjson::Value const *config_module) {
  auto str = get_string(&config_stream, "source");
  if (!str) {
    return;
  }
  source_name = str.v;
  str = get_string(&config_stream, "type");
  if (!str) {
    return;
  }
  type = str.v;
  if (auto x = get_uint(&config_stream, "array_size")) {
    array_size = size_t(x.v);
  }
  LOG(Sev::Debug,
      "HDFWriterModule::parse_config f142 source_name: {}  type: {}  "
      "array_size: {}",
      source_name, type, array_size);

  if (auto x = get_int(&config_stream, "nexus.indices.index_every_kb")) {
    index_every_bytes = uint64_t(x.v) * 1024;
  } else if (auto x = get_int(&config_stream, "nexus.indices.index_every_mb")) {
    index_every_bytes = uint64_t(x.v) * 1024 * 1024;
  }
}

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group& hdf_parent,
                          std::string hdf_parent_name,
                          rapidjson::Value const *attributes,
                          CollectiveQueue *cq) {

  auto hdf_group = hdf5::node::Group(hdf_parent.nodes[hdf_parent_name]);

  string s("value");
  impl.reset(impl_fac(static_cast<hid_t>(hdf_group), array_size, type, s, cq));
  if (!impl) {
    LOG(Sev::Error,
        "Could not create a writer implementation for value_type {}", type);
    return HDFWriterModule::InitResult::ERROR_IO();
  }
  this->ds_timestamp =
      h5::h5d_chunked_1d<uint64_t>::create(static_cast<hid_t>(hdf_group), "time", 64 * 1024, cq);
  this->ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::create(
      static_cast<hid_t>(hdf_group), "cue_timestamp_zero", 64 * 1024, cq);
  this->ds_cue_index = h5::h5d_chunked_1d<uint64_t>::create(
      static_cast<hid_t>(hdf_group), "cue_index", 64 * 1024, cq);
  if (!ds_timestamp || !ds_cue_timestamp_zero || !ds_cue_index) {
    impl.reset();
    return HDFWriterModule::InitResult::ERROR_IO();
  }
  if (do_writer_forwarder_internal) {
    this->ds_seq_data = h5::h5d_chunked_1d<uint64_t>::create(
        static_cast<hid_t>(hdf_group), source_name + "__fwdinfo_seq_data", 64 * 1024, cq);
    this->ds_seq_fwd = h5::h5d_chunked_1d<uint64_t>::create(
        static_cast<hid_t>(hdf_group), source_name + "__fwdinfo_seq_fwd", 64 * 1024, cq);
    this->ds_ts_data = h5::h5d_chunked_1d<uint64_t>::create(
        static_cast<hid_t>(hdf_group), source_name + "__fwdinfo_ts_data", 64 * 1024, cq);
    if (!ds_seq_data || !ds_seq_fwd || !ds_ts_data) {
      impl.reset();
      return HDFWriterModule::InitResult::ERROR_IO();
    }
  }
  if (attributes) {
    write_attributes(hdf_group, attributes);
  }
  return HDFWriterModule::InitResult::OK();
}

HDFWriterModule::InitResult HDFWriterModule::reopen(hid_t hdf_file,
                                                    std::string hdf_parent_name,
                                                    CollectiveQueue *cq,
                                                    HDFIDStore *hdf_store) {
  auto hdf_group = H5Gopen2(hdf_file, hdf_parent_name.data(), H5P_DEFAULT);
  if (hdf_group < 0) {
    LOG(Sev::Error, "can not open HDF group");
    return HDFWriterModule::InitResult::ERROR_IO();
  }

  string s("value");
  impl.reset(impl_fac_open(hdf_group, array_size, type, s, cq, hdf_store));
  if (!impl) {
    LOG(Sev::Error,
        "Could not create a writer implementation for value_type {}", type);
    return HDFWriterModule::InitResult::ERROR_IO();
  }

  this->ds_timestamp =
      h5::h5d_chunked_1d<uint64_t>::open(hdf_group, "time", cq, hdf_store);
  this->ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::open(
      hdf_group, "cue_timestamp_zero", cq, hdf_store);
  this->ds_cue_index =
      h5::h5d_chunked_1d<uint64_t>::open(hdf_group, "cue_index", cq, hdf_store);
  if (!ds_timestamp || !ds_cue_timestamp_zero || !ds_cue_index) {
    impl.reset();
    return HDFWriterModule::InitResult::ERROR_IO();
  }

  // TODO take from config
  size_t buffer_size = 1024 * 1024;
  size_t buffer_packet_max = 0;

  ds_timestamp->buffer_init(buffer_size, buffer_packet_max);
  ds_cue_timestamp_zero->buffer_init(buffer_size, buffer_packet_max);
  ds_cue_index->buffer_init(buffer_size, buffer_packet_max);

  if (do_writer_forwarder_internal) {
    this->ds_seq_data = h5::h5d_chunked_1d<uint64_t>::open(
        hdf_group, source_name + "__fwdinfo_seq_data", cq, hdf_store);
    this->ds_seq_fwd = h5::h5d_chunked_1d<uint64_t>::open(
        hdf_group, source_name + "__fwdinfo_seq_fwd", cq, hdf_store);
    this->ds_ts_data = h5::h5d_chunked_1d<uint64_t>::open(
        hdf_group, source_name + "__fwdinfo_ts_data", cq, hdf_store);
    if (!ds_seq_data || !ds_seq_fwd || !ds_ts_data) {
      impl.reset();
      return HDFWriterModule::InitResult::ERROR_IO();
    }
    ds_seq_data->buffer_init(buffer_size, buffer_packet_max);
    ds_seq_fwd->buffer_init(buffer_size, buffer_packet_max);
    ds_ts_data->buffer_init(buffer_size, buffer_packet_max);
  }

  H5Gclose(hdf_group);
  return HDFWriterModule::InitResult::OK();
}

HDFWriterModule::WriteResult HDFWriterModule::write(Msg const &msg) {
  auto fbuf = get_fbuf(msg.data());
  if (!impl) {
    LOG(Sev::Warning,
        "sorry, but we were unable to initialize for this kind of messages");
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

void HDFWriterModule::enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                                int mpi_rank) {
  this->cq = cq;
  ds_timestamp->ds.cq = cq;
  ds_timestamp->ds.hdf_store = hdf_store;
  ds_timestamp->ds.mpi_rank = mpi_rank;

  ds_cue_timestamp_zero->ds.cq = cq;
  ds_cue_timestamp_zero->ds.hdf_store = hdf_store;
  ds_cue_timestamp_zero->ds.mpi_rank = mpi_rank;

  ds_cue_index->ds.cq = cq;
  ds_cue_index->ds.hdf_store = hdf_store;
  ds_cue_index->ds.mpi_rank = mpi_rank;

  ds_seq_data->ds.cq = cq;
  ds_seq_data->ds.hdf_store = hdf_store;
  ds_seq_data->ds.mpi_rank = mpi_rank;

  ds_seq_fwd->ds.cq = cq;
  ds_seq_fwd->ds.hdf_store = hdf_store;
  ds_seq_fwd->ds.mpi_rank = mpi_rank;

  ds_ts_data->ds.cq = cq;
  ds_ts_data->ds.hdf_store = hdf_store;
  ds_ts_data->ds.mpi_rank = mpi_rank;
}

int32_t HDFWriterModule::flush() { return 0; }

int32_t HDFWriterModule::close() { return 0; }

HDFWriterModuleRegistry::Registrar
    g_registrar_HDFWriterModule("f142", HDFWriterModule::create);

} // namespace f142
} // namespace Schemas
} // namespace FileWriter
