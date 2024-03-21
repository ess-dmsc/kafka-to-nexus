// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatbufferMessage.h"
#include "MetaData/Value.h"
#include "NeXusDataset/NeXusDataset.h"
#include "WriterModuleBase.h"

namespace WriterModule {
namespace ev44 {

using FlatbufferMessage = FileWriter::FlatbufferMessage;
/// \brief Write module for the ev44 flatbuffer schema.
class ev44_Writer : public WriterModule::Base {
public:
  ev44_Writer()
      : WriterModule::Base("ev44", true, "NXevent_data"),
        EventsWrittenMetadataField("", "events") {}
  InitResult init_hdf(hdf5::node::Group &HDFGroup) override;
  WriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;

  /// \brief Write flatbuffer message.
  ///
  /// \param FlatBufferMessage
  void writeImpl(FlatbufferMessage const &Message) override;

  NeXusDataset::EventTimeOffset EventTimeOffset;
  NeXusDataset::EventId EventId;
  NeXusDataset::EventTimeZero EventTimeZero;
  NeXusDataset::EventIndex EventIndex;
  NeXusDataset::CueIndex CueIndex;
  NeXusDataset::CueTimestampZero CueTimestampZero;

  void register_meta_data(hdf5::node::Group const &HDFGroup,
                          MetaData::TrackerPtr const &Tracker) override;

  void setCueInterval(uint64_t interval) {
    CueInterval.setValue("", std::to_string(interval));
  }

private:
  JsonConfig::Field<int64_t> CueInterval{this, "cue_interval", 100'000'000};
  JsonConfig::Field<uint64_t> ChunkSize{this, "chunk_size", 1024 * 1024};
  int64_t EventsWritten{0};
  int64_t LastCueIndex{-1};
  MetaData::Value<int64_t> EventsWrittenMetadataField;

  // Buffer where ReferenceTimeIndex of every message is shifted by the number
  // of EventsWritten before storing it on file. Using a member variable to
  // reduce memory allocations.
  std::vector<int64_t> ShiftedReferenceTimeIndex;
};
} // namespace ev44
} // namespace WriterModule
