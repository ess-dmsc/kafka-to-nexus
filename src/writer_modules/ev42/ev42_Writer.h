// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "AdcDatasets.h"
#include "FlatbufferMessage.h"
#include "HDFWriterModule.h"
#include "NeXusDataset.h"

namespace Module {
namespace ev42 {

using FlatbufferMessage = FileWriter::FlatbufferMessage;

class ev42_Writer : public FileWriter::HDFWriterModule {
public:
  void parse_config(std::string const &ConfigurationStream) override;
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  HDFWriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void write(FlatbufferMessage const &Message) override;

  NeXusDataset::EventTimeOffset EventTimeOffset;
  NeXusDataset::EventId EventId;
  NeXusDataset::EventTimeZero EventTimeZero;
  NeXusDataset::EventIndex EventIndex;
  NeXusDataset::CueIndex CueIndex;
  NeXusDataset::CueTimestampZero CueTimestampZero;
  hsize_t ChunkSizeBytes = 1 << 16;
  uint64_t EventsWritten = 0;
  uint64_t LastEventIndex = 0;
  uint64_t EventIndexInterval = std::numeric_limits<uint64_t>::max();

private:
  void createAdcDatasets(hdf5::node::Group &HDFGroup) const;
  bool RecordAdcPulseDebugData = false;
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
};
} // namespace ev42
} // namespace Module
