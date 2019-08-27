// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <ev42_events_generated.h>

#include "../../HDFFile.h"
#include "../../helper.h"
#include "../../json.h"
#include "ev42_rw.h"

namespace FileWriter {
namespace Schemas {
namespace ev42 {

using nlohmann::json;

struct append_ret {
  int status;
  uint64_t written_bytes;
  uint64_t ix0;

  explicit operator bool() const { return status == 0; }
};

static EventMessage const *get_fbuf(char const *data) {
  return GetEventMessage(data);
}

bool FlatbufferReader::verify(FlatbufferMessage const &Message) const {
  flatbuffers::Verifier VerifierInstance(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyEventMessageBuffer(VerifierInstance);
}

std::string
FlatbufferReader::source_name(FlatbufferMessage const &Message) const {
  auto fbuf = get_fbuf(Message.data());
  auto NamePtr = fbuf->source_name();
  if (NamePtr == nullptr) {
    Logger->info("message has no source_name");
    return "";
  }
  return NamePtr->str();
}

uint64_t FlatbufferReader::timestamp(FlatbufferMessage const &Message) const {
  auto fbuf = get_fbuf(Message.data());
  return fbuf->pulse_time();
}

static FlatbufferReaderRegistry::Registrar<FlatbufferReader>
    RegisterReader("ev42");

void HDFWriterModule::parse_config(std::string const &ConfigurationStream,
                                   std::string const &) {
  auto ConfigurationStreamJson = json::parse(ConfigurationStream);
  try {
    index_every_bytes =
        ConfigurationStreamJson["nexus"]["indices"]["index_every_kb"]
            .get<uint64_t>() *
        1024;
    Logger->trace("index_every_bytes: {}", index_every_bytes);
  } catch (...) { /* it's ok if not found */
  }
  try {
    index_every_bytes =
        ConfigurationStreamJson["nexus"]["indices"]["index_every_mb"]
            .get<uint64_t>() *
        1024 * 1024;
    Logger->trace("index_every_bytes: {}", index_every_bytes);
  } catch (...) { /* it's ok if not found */
  }
  try {
    ConfigurationStreamJson["nexus"]["chunk"]["chunk_n_elements"]
        .get<uint64_t>();
    Logger->error("chunk_n_elements is no longer supported");
  } catch (...) { /* it's ok if not found */
  }
  try {
    chunk_bytes =
        ConfigurationStreamJson["nexus"]["chunk"]["chunk_kb"].get<uint64_t>() *
        1024;
    Logger->trace("chunk_bytes: {}", chunk_bytes);
  } catch (...) { /* it's ok if not found */
  }
  try {
    chunk_bytes =
        ConfigurationStreamJson["nexus"]["chunk"]["chunk_mb"].get<uint64_t>() *
        1024 * 1024;
    Logger->trace("chunk_bytes: {}", chunk_bytes);
  } catch (...) { /* it's ok if not found */
  }
  try {
    buffer_size =
        ConfigurationStreamJson["nexus"]["buffer"]["size_kb"].get<uint64_t>() *
        1024;
    Logger->trace("buffer_size: {}", buffer_size);
  } catch (...) { /* it's ok if not found */
  }
  try {
    buffer_size =
        ConfigurationStreamJson["nexus"]["buffer"]["size_mb"].get<uint64_t>() *
        1024 * 1024;
    Logger->trace("buffer_size: {}", buffer_size);
  } catch (...) { /* it's ok if not found */
  }
  try {
    buffer_packet_max =
        ConfigurationStreamJson["nexus"]["buffer"]["packet_max_kb"]
            .get<uint64_t>() *
        1024;
    Logger->trace("buffer_packet_max: {}", buffer_packet_max);
  } catch (...) { /* it's ok if not found */
  }
}

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &HDFGroup,
                          std::string const &HDFAttributes) {

  try {
    this->ds_event_time_offset = h5::h5d_chunked_1d<uint32_t>::create(
        HDFGroup, "event_time_offset", chunk_bytes);
    this->ds_event_id =
        h5::h5d_chunked_1d<uint32_t>::create(HDFGroup, "event_id", chunk_bytes);
    this->ds_event_time_zero = h5::h5d_chunked_1d<uint64_t>::create(
        HDFGroup, "event_time_zero", chunk_bytes);
    this->ds_event_index = h5::h5d_chunked_1d<uint32_t>::create(
        HDFGroup, "event_index", chunk_bytes);
    this->ds_cue_index = h5::h5d_chunked_1d<uint32_t>::create(
        HDFGroup, "cue_index", chunk_bytes);
    this->ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::create(
        HDFGroup, "cue_timestamp_zero", chunk_bytes);

    if (!ds_event_time_offset || !ds_event_id || !ds_event_time_zero ||
        !ds_event_index || !ds_cue_index || !ds_cue_timestamp_zero) {
      ds_event_time_offset.reset();
      ds_event_id.reset();
      ds_event_time_zero.reset();
      ds_event_index.reset();
      ds_cue_index.reset();
      ds_cue_timestamp_zero.reset();
      throw std::runtime_error("Dataset init failed");
    }
    if (HDFGroup.attributes.exists("NX_class")) {
      Logger->info("NX_class already specified!");
    } else {
      auto ClassAttribute = HDFGroup.attributes.create<std::string>("NX_class");
      ClassAttribute.write("NXevent_data");
    }
    auto AttributesJson = nlohmann::json::parse(HDFAttributes);
    writeAttributes(HDFGroup, &AttributesJson, Logger);
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    Logger->error("ev42 could not init hdf_parent: {}  trace: {}",
                  static_cast<std::string>(HDFGroup.link().path()), message);
    return HDFWriterModule::InitResult::ERROR;
  }
  return HDFWriterModule::InitResult::OK;
}

HDFWriterModule::InitResult
HDFWriterModule::reopen(hdf5::node::Group &HDFGroup) {
  try {
    this->ds_event_time_offset =
        h5::h5d_chunked_1d<uint32_t>::open(HDFGroup, "event_time_offset");
    this->ds_event_id =
        h5::h5d_chunked_1d<uint32_t>::open(HDFGroup, "event_id");
    this->ds_event_time_zero =
        h5::h5d_chunked_1d<uint64_t>::open(HDFGroup, "event_time_zero");
    this->ds_event_index =
        h5::h5d_chunked_1d<uint32_t>::open(HDFGroup, "event_index");
    this->ds_cue_index =
        h5::h5d_chunked_1d<uint32_t>::open(HDFGroup, "cue_index");
    this->ds_cue_timestamp_zero =
        h5::h5d_chunked_1d<uint64_t>::open(HDFGroup, "cue_timestamp_zero");
  } catch (std::exception &E) {
    Logger->error(
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return HDFWriterModule::InitResult::ERROR;
  }

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
    throw std::runtime_error(
        fmt::format("ev42 could not init hdf_parent: {}",
                    static_cast<std::string>(HDFGroup.link().path())));
  }
  return HDFWriterModule::InitResult::OK;
}

void HDFWriterModule::write(FlatbufferMessage const &Message) {
  if (!ds_event_time_offset) {
    throw FileWriter::HDFWriterModuleRegistry::WriterException(
        "Error, time of flight not present.");
  }
  auto fbuf = get_fbuf(Message.data());
  auto w1ret = this->ds_event_time_offset->append_data_1d(
      fbuf->time_of_flight()->data(), fbuf->time_of_flight()->size());
  auto w2ret = this->ds_event_id->append_data_1d(fbuf->detector_id()->data(),
                                                 fbuf->detector_id()->size());
  if (w1ret.ix0 != w2ret.ix0) {
    Logger->warn("written data lengths differ");
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

static HDFWriterModuleRegistry::Registrar<HDFWriterModule>
    RegisterWriter("ev42");

} // namespace ev42
} // namespace Schemas
} // namespace FileWriter
