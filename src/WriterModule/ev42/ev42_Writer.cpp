// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <ev42_events_generated.h>

#include "HDFOperations.h"
#include "WriterRegistrar.h"
#include "ev42_Writer.h"
#include "helper.h"
#include "json.h"

namespace {
template <typename DataType>
hdf5::ArrayAdapter<const DataType> const
getFBVectorAsArrayAdapter(const flatbuffers::Vector<DataType> *Data) {
  return {Data->data(), Data->size()};
}
} // namespace

namespace WriterModule {
namespace ev42 {

using nlohmann::json;
using HDFWriterModule = WriterModule::Base;

struct append_ret {
  int status;
  uint64_t written_bytes;
  uint64_t ix0;

  explicit operator bool() const { return status == 0; }
};

void ev42_Writer::createAdcDatasets(hdf5::node::Group &HDFGroup) const {
  NeXusDataset::Amplitude(        // NOLINT(bugprone-unused-raii)
      HDFGroup,                   // NOLINT(bugprone-unused-raii)
      NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
      ChunkSize);                 // NOLINT(bugprone-unused-raii)

  NeXusDataset::PeakArea(         // NOLINT(bugprone-unused-raii)
      HDFGroup,                   // NOLINT(bugprone-unused-raii)
      NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
      ChunkSize);                 // NOLINT(bugprone-unused-raii)

  NeXusDataset::Background(       // NOLINT(bugprone-unused-raii)
      HDFGroup,                   // NOLINT(bugprone-unused-raii)
      NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
      ChunkSize);                 // NOLINT(bugprone-unused-raii)

  NeXusDataset::ThresholdTime(    // NOLINT(bugprone-unused-raii)
      HDFGroup,                   // NOLINT(bugprone-unused-raii)
      NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
      ChunkSize);                 // NOLINT(bugprone-unused-raii)

  NeXusDataset::PeakTime(         // NOLINT(bugprone-unused-raii)
      HDFGroup,                   // NOLINT(bugprone-unused-raii)
      NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
      ChunkSize);                 // NOLINT(bugprone-unused-raii)
}

InitResult ev42_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  auto Create = NeXusDataset::Mode::Create;
  try {

    NeXusDataset::EventTimeOffset( // NOLINT(bugprone-unused-raii)
        HDFGroup,                  // NOLINT(bugprone-unused-raii)
        Create,                    // NOLINT(bugprone-unused-raii)
        ChunkSize);                // NOLINT(bugprone-unused-raii)

    NeXusDataset::EventId( // NOLINT(bugprone-unused-raii)
        HDFGroup,          // NOLINT(bugprone-unused-raii)
        Create,            // NOLINT(bugprone-unused-raii)
        ChunkSize);        // NOLINT(bugprone-unused-raii)

    NeXusDataset::EventTimeZero( // NOLINT(bugprone-unused-raii)
        HDFGroup,                // NOLINT(bugprone-unused-raii)
        Create,                  // NOLINT(bugprone-unused-raii)
        ChunkSize);              // NOLINT(bugprone-unused-raii)

    NeXusDataset::EventIndex( // NOLINT(bugprone-unused-raii)
        HDFGroup,             // NOLINT(bugprone-unused-raii)
        Create,               // NOLINT(bugprone-unused-raii)
        ChunkSize);           // NOLINT(bugprone-unused-raii)

    NeXusDataset::CueIndex( // NOLINT(bugprone-unused-raii)
        HDFGroup,           // NOLINT(bugprone-unused-raii)
        Create,             // NOLINT(bugprone-unused-raii)
        ChunkSize);         // NOLINT(bugprone-unused-raii)

    NeXusDataset::CueTimestampZero( // NOLINT(bugprone-unused-raii)
        HDFGroup,                   // NOLINT(bugprone-unused-raii)
        Create,                     // NOLINT(bugprone-unused-raii)
        ChunkSize);                 // NOLINT(bugprone-unused-raii)

    if (RecordAdcPulseDebugData) {
      createAdcDatasets(HDFGroup);
    }
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    Logger->error("ev42 could not init_hdf hdf_parent: {}  trace: {}",
                  static_cast<std::string>(HDFGroup.link().path()), message);
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

WriterModule::InitResult ev42_Writer::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    EventTimeOffset = NeXusDataset::EventTimeOffset(HDFGroup, Open);
    EventId = NeXusDataset::EventId(HDFGroup, Open);
    EventTimeZero = NeXusDataset::EventTimeZero(HDFGroup, Open);
    EventIndex = NeXusDataset::EventIndex(HDFGroup, Open);
    CueIndex = NeXusDataset::CueIndex(HDFGroup, Open);
    CueTimestampZero = NeXusDataset::CueTimestampZero(HDFGroup, Open);
    if (RecordAdcPulseDebugData) {
      reopenAdcDatasets(HDFGroup);
    }
  } catch (std::exception &E) {
    Logger->error(
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}
void ev42_Writer::reopenAdcDatasets(const hdf5::node::Group &HDFGroup) {
  AmplitudeDataset =
      NeXusDataset::Amplitude(HDFGroup, NeXusDataset::Mode::Open);
  PeakAreaDataset = NeXusDataset::PeakArea(HDFGroup, NeXusDataset::Mode::Open);
  BackgroundDataset =
      NeXusDataset::Background(HDFGroup, NeXusDataset::Mode::Open);
  ThresholdTimeDataset =
      NeXusDataset::ThresholdTime(HDFGroup, NeXusDataset::Mode::Open);
  PeakTimeDataset = NeXusDataset::PeakTime(HDFGroup, NeXusDataset::Mode::Open);
}

void ev42_Writer::write(FlatbufferMessage const &Message) {
  auto EventMsgFlatbuffer = GetEventMessage(Message.data());
  EventTimeOffset.appendArray(
      getFBVectorAsArrayAdapter(EventMsgFlatbuffer->time_of_flight()));
  EventId.appendArray(
      getFBVectorAsArrayAdapter(EventMsgFlatbuffer->detector_id()));
  if (EventMsgFlatbuffer->time_of_flight()->size() !=
      EventMsgFlatbuffer->detector_id()->size()) {
    Logger->warn("written data lengths differ");
  }
  auto CurrentRefTime = EventMsgFlatbuffer->pulse_time();
  auto CurrentNumberOfEvents = EventMsgFlatbuffer->detector_id()->size();
  EventTimeZero.appendElement(CurrentRefTime);
  EventIndex.appendElement(EventsWritten);
  EventsWritten += CurrentNumberOfEvents;
  if (EventsWritten > LastEventIndex + EventIndexInterval) {
    auto LastRefTimeOffset = EventMsgFlatbuffer->time_of_flight()->operator[](
        CurrentNumberOfEvents - 1);
    CueTimestampZero.appendElement(CurrentRefTime + LastRefTimeOffset);
    CueIndex.appendElement(EventsWritten - 1);
    LastEventIndex = EventsWritten - 1;
  }

  if (RecordAdcPulseDebugData) {
    writeAdcPulseData(Message);
  }
}

void ev42_Writer::writeAdcPulseData(FlatbufferMessage const &Message) {
  auto EventMsgFlatbuffer = GetEventMessage(Message.data());
  if (EventMsgFlatbuffer->facility_specific_data_type() !=
      FacilityData::AdcPulseDebug) {
    padDatasetsWithZeroesEqualToNumberOfEvents(Message);
  } else {
    writeAdcPulseDataFromMessageToFile(Message);
  }
}

void ev42_Writer::writeAdcPulseDataFromMessageToFile(
    FlatbufferMessage const &Message) {
  auto EventMsgFlatbuffer = GetEventMessage(Message.data());
  auto AdcPulseDebugMsgFlatbuffer =
      EventMsgFlatbuffer->facility_specific_data_as_AdcPulseDebug();

  hdf5::ArrayAdapter<const uint32_t> AmplitudeArray =
      getFBVectorAsArrayAdapter(AdcPulseDebugMsgFlatbuffer->amplitude());
  AmplitudeDataset.appendArray(AmplitudeArray);

  hdf5::ArrayAdapter<const uint32_t> PeakAreaArray =
      getFBVectorAsArrayAdapter(AdcPulseDebugMsgFlatbuffer->peak_area());
  PeakAreaDataset.appendArray(PeakAreaArray);

  hdf5::ArrayAdapter<const uint32_t> BackgroundArray =
      getFBVectorAsArrayAdapter(AdcPulseDebugMsgFlatbuffer->background());
  BackgroundDataset.appendArray(BackgroundArray);

  hdf5::ArrayAdapter<const uint64_t> ThresholdTimeArray =
      getFBVectorAsArrayAdapter(AdcPulseDebugMsgFlatbuffer->threshold_time());
  ThresholdTimeDataset.appendArray(ThresholdTimeArray);

  hdf5::ArrayAdapter<const uint64_t> PeakTimeArray =
      getFBVectorAsArrayAdapter(AdcPulseDebugMsgFlatbuffer->peak_time());
  PeakTimeDataset.appendArray(PeakTimeArray);
}

/// If ADC pulse data is missing from message then pad the datasets so that
/// event_index and event_time_zero datasets will still be consistent with ADC
/// datasets
void ev42_Writer::padDatasetsWithZeroesEqualToNumberOfEvents(
    FlatbufferMessage const &Message) {
  auto EventMsgFlatbuffer = GetEventMessage(Message.data());
  size_t NumberOfEventsInMessage = EventMsgFlatbuffer->time_of_flight()->size();
  std::vector<uint32_t> ZeroesUInt32(NumberOfEventsInMessage, 0);
  hdf5::ArrayAdapter<const uint32_t> ZeroesUInt32ArrayAdapter(
      ZeroesUInt32.data(), ZeroesUInt32.size());
  std::vector<uint64_t> ZeroesUInt64(NumberOfEventsInMessage, 0);
  hdf5::ArrayAdapter<const uint64_t> ZeroesUInt64ArrayAdapter(
      ZeroesUInt64.data(), ZeroesUInt64.size());

  AmplitudeDataset.appendArray(ZeroesUInt32ArrayAdapter);
  PeakAreaDataset.appendArray(ZeroesUInt32ArrayAdapter);
  BackgroundDataset.appendArray(ZeroesUInt32ArrayAdapter);
  ThresholdTimeDataset.appendArray(ZeroesUInt64ArrayAdapter);
  PeakTimeDataset.appendArray(ZeroesUInt64ArrayAdapter);
}

static WriterModule::Registry::Registrar<ev42_Writer> RegisterWriter("ev42",
                                                                     "ev42");

} // namespace ev42
} // namespace WriterModule
