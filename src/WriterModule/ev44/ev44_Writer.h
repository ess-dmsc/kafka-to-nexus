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
  InitResult init_hdf(hdf5::node::Group &HDFGroup) const override;
  WriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;

  /// \brief Write flatbuffer message.
  ///
  /// \param FlatBufferMessage
  void writeImpl(FlatbufferMessage const &Message) override;

  NeXusDataset::EventTimeOffset EventTimeOffset;
  NeXusDataset::EventId EventId;
  NeXusDataset::EventTimeZero EventTimeZero;
  NeXusDataset::EventTimeZeroIndex EventTimeZeroIndex;
  NeXusDataset::EventIndex EventIndex;
  NeXusDataset::CueIndex CueIndex;
  NeXusDataset::CueTimestampZero CueTimestampZero;

  void register_meta_data(hdf5::node::Group const &HDFGroup,
                          MetaData::TrackerPtr const &Tracker) override;

private:
  void
  padDatasetsWithZeroesEqualToNumberOfEvents(FlatbufferMessage const &Message);

  JsonConfig::Field<uint64_t> EventIndexInterval{
      this, "cue_interval", std::numeric_limits<uint64_t>::max()};
  JsonConfig::Field<uint64_t> ChunkSize{this, "chunk_size", 1 << 20};
  uint64_t EventsWritten{0};
  uint64_t LastEventIndex{0};
  MetaData::Value<uint64_t> EventsWrittenMetadataField;
};
} // namespace ev44
} // namespace WriterModule
