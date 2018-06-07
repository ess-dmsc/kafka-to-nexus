#include "f142_rw.h"
#include "../../CollectiveQueue.h"
#include "../../HDFFile.h"
#include "../../helper.h"
#include "../../json.h"
#include "FlatbufferReader.h"
#include "WriterArray.h"
#include "WriterScalar.h"
#include <hdf5.h>
#include <limits>

namespace FileWriter {
namespace Schemas {
namespace f142 {

using nlohmann::json;

enum class Rank {
  SCALAR,
  ARRAY,
};

static std::map<Rank, std::map<std::string, std::unique_ptr<WriterFactory>>>
    name_to_value_traits;

namespace {

struct InitTypeMap {
  InitTypeMap() {
    // name_to_value_traits[Rank::SCALAR];
    // name_to_value_traits[Rank::ARRAY];
    auto &Scalar = name_to_value_traits[Rank::SCALAR];
    auto &Array = name_to_value_traits[Rank::ARRAY];
    // clang-format off
  Scalar[ "uint8"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar< uint8_t, UByte>);
  Scalar["uint16"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<uint16_t, UShort>);
  Scalar["uint32"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<uint32_t, UInt>);
  Scalar["uint64"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<uint64_t, ULong>);
  Scalar[  "int8"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<  int8_t, Byte>);
  Scalar[ "int16"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar< int16_t, Short>);
  Scalar[ "int32"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar< int32_t, Int>);
  Scalar[ "int64"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar< int64_t, Long>);
  Scalar[ "float"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<   float, Float>);
  Scalar["double"] = std::unique_ptr<WriterFactory>(new WriterFactoryScalar<  double, Double>);

  Array[ "uint8"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray< uint8_t, ArrayUByte>);
  Array["uint16"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<uint16_t, ArrayUShort>);
  Array["uint32"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<uint32_t, ArrayUInt>);
  Array["uint64"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<uint64_t, ArrayULong>);
  Array[  "int8"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<  int8_t, ArrayByte>);
  Array[ "int16"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray< int16_t, ArrayShort>);
  Array[ "int32"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray< int32_t, ArrayInt>);
  Array[ "int64"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray< int64_t, ArrayLong>);
  Array[ "float"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<   float, ArrayFloat>);
  Array["double"] = std::unique_ptr<WriterFactory>(new WriterFactoryArray<  double, ArrayDouble>);
    // clang-format on
  }
};

InitTypeMap TriggerInitTypeMap;
}

std::unique_ptr<WriterTypedBase>
impl_fac(hdf5::node::Group HDFGroup, size_t ArraySize, std::string TypeName,
         std::string SourceName, CollectiveQueue *cq) {
  Rank TheRank = Rank::SCALAR;
  if (ArraySize > 0) {
    TheRank = Rank::ARRAY;
  }
  auto ValueTraitsMaybe = name_to_value_traits[TheRank].find(TypeName);
  if (ValueTraitsMaybe == name_to_value_traits[TheRank].end()) {
    return nullptr;
  }
  auto &ValueTraits = ValueTraitsMaybe->second;
  return ValueTraits->createWriter(HDFGroup, SourceName, ArraySize,
                                   ValueTraits->getValueUnionID(), cq);
}

std::unique_ptr<WriterTypedBase>
impl_fac_open(hdf5::node::Group HDFGroup, size_t ArraySize,
              std::string TypeName, std::string SourceName, CollectiveQueue *cq,
              HDFIDStore *HDFStore) {
  Rank TheRank = Rank::SCALAR;
  if (ArraySize > 0) {
    TheRank = Rank::ARRAY;
  }
  auto ValueTraitsMaybe = name_to_value_traits[TheRank].find(TypeName);
  if (ValueTraitsMaybe == name_to_value_traits[TheRank].end()) {
    return nullptr;
  }
  auto &ValueTraits = ValueTraitsMaybe->second;
  if (!ValueTraits) {
    LOG(Sev::Critical, "Could not get ValueTraits for TypeName: {}  ArraySize: "
                       "{}  name_to_value_traits.size(): {}",
        TypeName, ArraySize, name_to_value_traits[TheRank].size());
  }
  return ValueTraits->createWriter(HDFGroup, SourceName, ArraySize,
                                   ValueTraits->getValueUnionID(), cq,
                                   HDFStore);
}

void HDFWriterModule::parse_config(std::string const &ConfigurationStream,
                                   std::string const &ConfigurationModule) {
  auto ConfigurationStreamJson = json::parse(ConfigurationStream);
  auto str = find<std::string>("source", ConfigurationStreamJson);
  if (!str) {
    return;
  }
  source_name = str.inner();

  str = find<std::string>("type", ConfigurationStreamJson);
  if (!str) {
    return;
  }
  type = str.inner();

  if (auto x = find<uint64_t>("array_size", ConfigurationStreamJson)) {
    array_size = size_t(x.inner());
  }
  LOG(Sev::Debug,
      "HDFWriterModule::parse_config f142 source_name: {}  type: {}  "
      "array_size: {}",
      source_name, type, array_size);

  try {
    index_every_bytes =
        ConfigurationStreamJson["nexus"]["indices"]["index_every_kb"]
            .get<uint64_t>() *
        1024;
    LOG(Sev::Debug, "index_every_bytes: {}", index_every_bytes);
  } catch (...) { /* it's ok if not found */
  }
  try {
    index_every_bytes =
        ConfigurationStreamJson["nexus"]["indices"]["index_every_mb"]
            .get<uint64_t>() *
        1024 * 1024;
    LOG(Sev::Debug, "index_every_bytes: {}", index_every_bytes);
  } catch (...) { /* it's ok if not found */
  }
}

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &HDFGroup,
                          std::string const &HDFAttributes) {
  // Keep these for now, experimenting with those on another branch.
  CollectiveQueue *cq = nullptr;
  try {
    std::string s("value");
    impl = impl_fac(HDFGroup, array_size, type, s, cq);

    if (!impl) {
      LOG(Sev::Error,
          "Could not create a writer implementation for value_type {}", type);
      return HDFWriterModule::InitResult::ERROR_IO();
    }
    this->ds_timestamp =
        h5::h5d_chunked_1d<uint64_t>::create(HDFGroup, "time", 64 * 1024, cq);
    this->ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::create(
        HDFGroup, "cue_timestamp_zero", 64 * 1024, cq);
    this->ds_cue_index = h5::h5d_chunked_1d<uint64_t>::create(
        HDFGroup, "cue_index", 64 * 1024, cq);
    if (!ds_timestamp || !ds_cue_timestamp_zero || !ds_cue_index) {
      impl.reset();
      return HDFWriterModule::InitResult::ERROR_IO();
    }
    if (do_writer_forwarder_internal) {
      this->ds_seq_data = h5::h5d_chunked_1d<uint64_t>::create(
          HDFGroup, source_name + "__fwdinfo_seq_data", 64 * 1024, cq);
      this->ds_seq_fwd = h5::h5d_chunked_1d<uint64_t>::create(
          HDFGroup, source_name + "__fwdinfo_seq_fwd", 64 * 1024, cq);
      this->ds_ts_data = h5::h5d_chunked_1d<uint64_t>::create(
          HDFGroup, source_name + "__fwdinfo_ts_data", 64 * 1024, cq);
      if (!ds_seq_data || !ds_seq_fwd || !ds_ts_data) {
        impl.reset();
        return HDFWriterModule::InitResult::ERROR_IO();
      }
    }
    auto AttributesJson = nlohmann::json::parse(HDFAttributes);
    HDFFile::write_attributes(HDFGroup, &AttributesJson);
  } catch (std::exception &e) {
    auto message = hdf5::error::print_nested(e);
    LOG(Sev::Error, "ERROR f142 could not init HDFGroup: {}  trace: {}",
        static_cast<std::string>(HDFGroup.link().path()), message);
  }
  return HDFWriterModule::InitResult::OK();
}

HDFWriterModule::InitResult
HDFWriterModule::reopen(hdf5::node::Group &HDFGroup) {
  // Keep these for now, experimenting with those on another branch.
  CollectiveQueue *cq = nullptr;
  HDFIDStore *hdf_store = nullptr;
  std::string s("value");
  impl = impl_fac_open(HDFGroup, array_size, type, s, cq, hdf_store);
  if (!impl) {
    LOG(Sev::Error,
        "Could not create a writer implementation for value_type {}", type);
    return HDFWriterModule::InitResult::ERROR_IO();
  }

  this->ds_timestamp =
      h5::h5d_chunked_1d<uint64_t>::open(HDFGroup, "time", cq, hdf_store);
  this->ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::open(
      HDFGroup, "cue_timestamp_zero", cq, hdf_store);
  this->ds_cue_index =
      h5::h5d_chunked_1d<uint64_t>::open(HDFGroup, "cue_index", cq, hdf_store);
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
        HDFGroup, source_name + "__fwdinfo_seq_data", cq, hdf_store);
    this->ds_seq_fwd = h5::h5d_chunked_1d<uint64_t>::open(
        HDFGroup, source_name + "__fwdinfo_seq_fwd", cq, hdf_store);
    this->ds_ts_data = h5::h5d_chunked_1d<uint64_t>::open(
        HDFGroup, source_name + "__fwdinfo_ts_data", cq, hdf_store);
    if (!ds_seq_data || !ds_seq_fwd || !ds_ts_data) {
      impl.reset();
      return HDFWriterModule::InitResult::ERROR_IO();
    }
    ds_seq_data->buffer_init(buffer_size, buffer_packet_max);
    ds_seq_fwd->buffer_init(buffer_size, buffer_packet_max);
    ds_ts_data->buffer_init(buffer_size, buffer_packet_max);
  }

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

static HDFWriterModuleRegistry::Registrar<HDFWriterModule>
    RegisterWriter("f142");

} // namespace f142
} // namespace Schemas
} // namespace FileWriter
