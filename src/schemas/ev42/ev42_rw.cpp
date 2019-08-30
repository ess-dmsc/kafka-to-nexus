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

namespace {
std::string const AdcGroupName = "adc_pulse_debug";
}

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

bool FlatbufferReader::verify(FlatbufferMessage const &Message) const {
  flatbuffers::Verifier VerifierInstance(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyEventMessageBuffer(VerifierInstance);
}

std::string
FlatbufferReader::source_name(FlatbufferMessage const &Message) const {
  auto fbuf = GetEventMessage(Message.data());
  auto NamePtr = fbuf->source_name();
  if (NamePtr == nullptr) {
    Logger->info("message has no source_name");
    return "";
  }
  return NamePtr->str();
}

uint64_t FlatbufferReader::timestamp(FlatbufferMessage const &Message) const {
  auto fbuf = GetEventMessage(Message.data());
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
  try {
    RecordAdcPulseDebugData =
        ConfigurationStreamJson["adc_pulse_debug"].get<bool>();
    Logger->trace("adc_pulse_debug: {}", RecordAdcPulseDebugData);
  } catch (...) { /* it's ok if not found */
  }
}

void HDFWriterModule::createAdcGroupAndDatasets(hdf5::node::Group &HDFGroup) {
  auto AdcGroup = HDFGroup.create_group(AdcGroupName);

  // bytes to number of elements
  size_t ChunkSizeFor32BitTypes = chunk_bytes/8;
  size_t ChunkSizeFor64BitTypes = chunk_bytes/16;

  NeXusDataset::Amplitude(        // NOLINT(bugprone-unused-raii)
      AdcGroup,                   // NOLINT(bugprone-unused-raii)
      NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
      ChunkSizeFor32BitTypes);    // NOLINT(bugprone-unused-raii)

  NeXusDataset::PeakArea(         // NOLINT(bugprone-unused-raii)
      AdcGroup,                   // NOLINT(bugprone-unused-raii)
      NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
      ChunkSizeFor32BitTypes);    // NOLINT(bugprone-unused-raii)

  NeXusDataset::Background(       // NOLINT(bugprone-unused-raii)
      AdcGroup,                   // NOLINT(bugprone-unused-raii)
      NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
      ChunkSizeFor32BitTypes);    // NOLINT(bugprone-unused-raii)

  NeXusDataset::ThresholdTime(    // NOLINT(bugprone-unused-raii)
      AdcGroup,                   // NOLINT(bugprone-unused-raii)
      NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
      ChunkSizeFor64BitTypes);    // NOLINT(bugprone-unused-raii)

  NeXusDataset::PeakTime(         // NOLINT(bugprone-unused-raii)
      AdcGroup,                   // NOLINT(bugprone-unused-raii)
      NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
      ChunkSizeFor64BitTypes);    // NOLINT(bugprone-unused-raii)
}

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &HDFGroup,
                          std::string const &HDFAttributes) {

  try {
    ds_event_time_offset = h5::h5d_chunked_1d<uint32_t>::create(
        HDFGroup, "event_time_offset", chunk_bytes);
    ds_event_id =
        h5::h5d_chunked_1d<uint32_t>::create(HDFGroup, "event_id", chunk_bytes);
    ds_event_time_zero = h5::h5d_chunked_1d<uint64_t>::create(
        HDFGroup, "event_time_zero", chunk_bytes);
    ds_event_index = h5::h5d_chunked_1d<uint32_t>::create(
        HDFGroup, "event_index", chunk_bytes);
    ds_cue_index = h5::h5d_chunked_1d<uint32_t>::create(HDFGroup, "cue_index",
                                                        chunk_bytes);
    ds_cue_timestamp_zero = h5::h5d_chunked_1d<uint64_t>::create(
        HDFGroup, "cue_timestamp_zero", chunk_bytes);

    if (RecordAdcPulseDebugData) {
      createAdcGroupAndDatasets(HDFGroup);
    }

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
    ds_event_time_offset =
        h5::h5d_chunked_1d<uint32_t>::open(HDFGroup, "event_time_offset");
    ds_event_id = h5::h5d_chunked_1d<uint32_t>::open(HDFGroup, "event_id");
    ds_event_time_zero =
        h5::h5d_chunked_1d<uint64_t>::open(HDFGroup, "event_time_zero");
    ds_event_index =
        h5::h5d_chunked_1d<uint32_t>::open(HDFGroup, "event_index");
    ds_cue_index = h5::h5d_chunked_1d<uint32_t>::open(HDFGroup, "cue_index");
    ds_cue_timestamp_zero =
        h5::h5d_chunked_1d<uint64_t>::open(HDFGroup, "cue_timestamp_zero");
    if (RecordAdcPulseDebugData) {
      reopenAdcDatasets(HDFGroup);
    }
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
void HDFWriterModule::reopenAdcDatasets(const hdf5::node::Group &HDFGroup) {
  hdf5::node::Group AdcGroup = HDFGroup[AdcGroupName];
  AmplitudeDataset =
      NeXusDataset::Amplitude(AdcGroup, NeXusDataset::Mode::Open);
  PeakAreaDataset = NeXusDataset::PeakArea(AdcGroup, NeXusDataset::Mode::Open);
  BackgroundDataset =
      NeXusDataset::Background(AdcGroup, NeXusDataset::Mode::Open);
  ThresholdTimeDataset =
      NeXusDataset::ThresholdTime(AdcGroup, NeXusDataset::Mode::Open);
  PeakTimeDataset = NeXusDataset::PeakTime(AdcGroup, NeXusDataset::Mode::Open);
}

void HDFWriterModule::write(FlatbufferMessage const &Message) {
  if (!ds_event_time_offset) {
    throw FileWriter::HDFWriterModuleRegistry::WriterException(
        "Error, time of flight not present.");
  }
  auto EventMsgFlatbuffer = GetEventMessage(Message.data());
  auto EventTimeAppendResultInfo = ds_event_time_offset->append_data_1d(
      EventMsgFlatbuffer->time_of_flight()->data(),
      EventMsgFlatbuffer->time_of_flight()->size());
  auto EventIdAppendResultInfo =
      ds_event_id->append_data_1d(EventMsgFlatbuffer->detector_id()->data(),
                                  EventMsgFlatbuffer->detector_id()->size());
  if (EventTimeAppendResultInfo.ix0 != EventIdAppendResultInfo.ix0) {
    Logger->warn("written data lengths differ");
  }
  auto pulse_time = EventMsgFlatbuffer->pulse_time();
  ds_event_time_zero->append_data_1d(&pulse_time, 1);
  uint32_t event_index = EventTimeAppendResultInfo.ix0;
  ds_event_index->append_data_1d(&event_index, 1);
  total_written_bytes += EventTimeAppendResultInfo.written_bytes;
  ts_max = std::max(pulse_time, ts_max);
  if (total_written_bytes > index_at_bytes + index_every_bytes) {
    ds_cue_timestamp_zero->append_data_1d(&ts_max, 1);
    ds_cue_index->append_data_1d(&event_index, 1);
    index_at_bytes = total_written_bytes;
  }

  if (RecordAdcPulseDebugData) {
    writeAdcPulseData(Message);
  }
}
void HDFWriterModule::writeAdcPulseData(FlatbufferMessage const &Message) {
  auto EventMsgFlatbuffer = GetEventMessage(Message.data());
  if (EventMsgFlatbuffer->facility_specific_data_type() !=
      FacilityData::AdcPulseDebug) {
    return;
  }
  auto AdcPulseDebugMsgFlatbuffer =
      EventMsgFlatbuffer->facility_specific_data_as_AdcPulseDebug();

  auto AmplitudePtr = AdcPulseDebugMsgFlatbuffer->amplitude()->data();
  auto AmplitudeSize = AdcPulseDebugMsgFlatbuffer->amplitude()->size();
  ArrayAdapter<const uint32_t> AmplitudeArray(AmplitudePtr, AmplitudeSize);
  AmplitudeDataset.appendArray(AmplitudeArray);

  auto PeakAreaPtr = AdcPulseDebugMsgFlatbuffer->peak_area()->data();
  auto PeakAreaSize = AdcPulseDebugMsgFlatbuffer->peak_area()->size();
  ArrayAdapter<const uint32_t> PeakAreaArray(PeakAreaPtr, PeakAreaSize);
  PeakAreaDataset.appendArray(PeakAreaArray);

  auto BackgroundPtr = AdcPulseDebugMsgFlatbuffer->background()->data();
  auto BackgroundSize = AdcPulseDebugMsgFlatbuffer->background()->size();
  ArrayAdapter<const uint32_t> BackgroundArray(BackgroundPtr, BackgroundSize);
  BackgroundDataset.appendArray(BackgroundArray);

  auto ThresholdTimePtr = AdcPulseDebugMsgFlatbuffer->threshold_time()->data();
  auto ThresholdTimeSize = AdcPulseDebugMsgFlatbuffer->threshold_time()->size();
  ArrayAdapter<const uint64_t> ThresholdTimeArray(ThresholdTimePtr,
                                                  ThresholdTimeSize);
  ThresholdTimeDataset.appendArray(ThresholdTimeArray);

  auto PeakTimePtr = AdcPulseDebugMsgFlatbuffer->peak_time()->data();
  auto PeakTimeSize = AdcPulseDebugMsgFlatbuffer->peak_time()->size();
  ArrayAdapter<const uint64_t> PeakTimeArray(PeakTimePtr, PeakTimeSize);
  PeakTimeDataset.appendArray(PeakTimeArray);
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
