// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include "AdcDatasets.h"
#include "NeXusDataset.h"

namespace FileWriter {
namespace Schemas {
namespace ev42 {

class FlatbufferReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;

private:
  SharedLogger Logger = spdlog::get("filewriterlogger");
  // add in others
};

class HDFWriterModule : public FileWriter::HDFWriterModule {
public:
  void parse_config(std::string const &ConfigurationStream) override;
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  HDFWriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void write(FlatbufferMessage const &Message) override;
  int32_t close() override;

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
} // namespace Schemas
} // namespace FileWriter
