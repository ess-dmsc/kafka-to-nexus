// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatbufferMessage.h"
#include "NeXusDataset/AdcDatasets.h"
#include "NeXusDataset/NeXusDataset.h"
#include "WriterModuleBase.h"

namespace WriterModule {
namespace ev42 {

using FlatbufferMessage = FileWriter::FlatbufferMessage;

class ev42_Writer : public WriterModule::Base {
public:
  void process_config() override;
  ev42_Writer() : WriterModule::Base(true, "NXevent_data") {}
  InitResult init_hdf(hdf5::node::Group &HDFGroup) override;
  WriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void write(FlatbufferMessage const &Message) override;

  NeXusDataset::EventTimeOffset EventTimeOffset;
  NeXusDataset::EventId EventId;
  NeXusDataset::EventTimeZero EventTimeZero;
  NeXusDataset::EventIndex EventIndex;
  NeXusDataset::CueIndex CueIndex;
  NeXusDataset::CueTimestampZero CueTimestampZero;
  uint64_t EventsWritten = 0;
  uint64_t LastEventIndex = 0;

private:
  void createAdcDatasets(hdf5::node::Group &HDFGroup) const;
  NeXusDataset::Amplitude AmplitudeDataset;
  NeXusDataset::PeakArea PeakAreaDataset;
  NeXusDataset::Background BackgroundDataset;
  NeXusDataset::ThresholdTime ThresholdTimeDataset;
  NeXusDataset::PeakTime PeakTimeDataset;
  SharedLogger Logger = spdlog::get("filewriterlogger");
  void reopenAdcDatasets(const hdf5::node::Group &HDFGroup);
  void writeAdcPulseData(FlatbufferMessage const &Message);
  void
  padDatasetsWithZeroesEqualToNumberOfEvents(FlatbufferMessage const &Message);
  void writeAdcPulseDataFromMessageToFile(FlatbufferMessage const &Message);

  WriterModuleConfig::Field<uint64_t> EventIndexInterval{this, "cue_interval", std::numeric_limits<uint64_t>::max()};
  WriterModuleConfig::Field<uint64_t> ChunkSize{this, "chunk_size", 16384};
  WriterModuleConfig::Field<bool> RecordAdcPulseDebugData{this, "adc_pulse_debug", false};
};
} // namespace ev42
} // namespace WriterModule
