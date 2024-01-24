// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <ev44_events_generated.h>

#include "HDFOperations.h"
#include "WriterRegistrar.h"
#include "ev44_Writer.h"
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
namespace ev44 {

using nlohmann::json;

InitResult ev44_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
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

  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("ev44 could not init_hdf hdf_parent: {}  trace: {}",
              static_cast<std::string>(HDFGroup.link().path()), message);
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

WriterModule::InitResult ev44_Writer::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    EventTimeOffset = NeXusDataset::EventTimeOffset(HDFGroup, Open);
    EventId = NeXusDataset::EventId(HDFGroup, Open);
    EventTimeZero = NeXusDataset::EventTimeZero(HDFGroup, Open);
    EventIndex = NeXusDataset::EventIndex(HDFGroup, Open);
    CueIndex = NeXusDataset::CueIndex(HDFGroup, Open);
    CueTimestampZero = NeXusDataset::CueTimestampZero(HDFGroup, Open);
  } catch (std::exception &E) {
    LOG_ERROR(
        R"(Failed to reopen datasets in HDF file with error message: "{}")",
        std::string(E.what()));
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

void ev44_Writer::writeImpl(FlatbufferMessage const &Message) {
  auto EventMsgFlatbuffer = GetEvent44Message(Message.data());
  EventTimeOffset.appendArray(
      getFBVectorAsArrayAdapter(EventMsgFlatbuffer->time_of_flight()));
  EventId.appendArray(
      getFBVectorAsArrayAdapter(EventMsgFlatbuffer->pixel_id()));
  auto CurrentNumberOfEvents = EventMsgFlatbuffer->pixel_id()->size();
  if (EventMsgFlatbuffer->time_of_flight()->size() != CurrentNumberOfEvents) {
    LOG_WARN("ev44 message data lengths differ (time_of_flight={} pixel_id={})",
             EventMsgFlatbuffer->time_of_flight()->size(),
             CurrentNumberOfEvents);
  }
  const flatbuffers::Vector<int64_t> *CurrentRefTime =
      EventMsgFlatbuffer->reference_time();

  EventTimeZero.appendArray(getFBVectorAsArrayAdapter(CurrentRefTime));

  // Shift incoming reference_time_index by the number of events already stored
  auto MessageReferenceTimeIndex = EventMsgFlatbuffer->reference_time_index();
  std::vector<uint32_t> ModifiedReferenceTimeIndex;
  ModifiedReferenceTimeIndex.reserve(MessageReferenceTimeIndex->size());
  for (auto value : *MessageReferenceTimeIndex) {
    ModifiedReferenceTimeIndex.push_back(value + EventsWritten);
  }
  EventIndex.appendArray(ModifiedReferenceTimeIndex);

  EventsWritten += CurrentNumberOfEvents;
  if (EventsWritten > LastCueIndex + CueInterval) {
    auto LastRefTimeOffset = EventMsgFlatbuffer->time_of_flight()->operator[](
        CurrentNumberOfEvents - 1);
    CueTimestampZero.appendElement(*(CurrentRefTime->end() - 1) +
                                   LastRefTimeOffset);
    CueIndex.appendElement(EventsWritten - 1);
    LastCueIndex = EventsWritten - 1;
  }
  EventsWrittenMetadataField.setValue(EventsWritten);
}

void ev44_Writer::register_meta_data(const hdf5::node::Group &HDFGroup,
                                     const MetaData::TrackerPtr &Tracker) {
  EventsWrittenMetadataField = MetaData::Value<uint64_t>(HDFGroup, "events");
  Tracker->registerMetaData(EventsWrittenMetadataField);
}

static WriterModule::Registry::Registrar<ev44_Writer> RegisterWriter("ev44",
                                                                     "ev44");

} // namespace ev44
} // namespace WriterModule
