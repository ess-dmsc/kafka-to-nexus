// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

// This filename is chosen such that it shows up in searches after the
// case-sensitive flatbuffer schema identifier.

#include <dtdb_adc_pulse_debug_generated.h>
#include <ev42_events_generated.h>
#include <gmock/gmock.h>

#include <utility>

#include "AccessMessageMetadata/ev42/ev42_Extractor.h"
#include "helpers/SetExtractorModule.h"

using FileWriter::FlatbufferReaderRegistry::ReaderPtr;

struct AdcDebugInfo {
  explicit AdcDebugInfo(std::vector<uint32_t> Amplitude = {0, 1, 2},
                        std::vector<uint32_t> PeakArea = {0, 1, 2},
                        std::vector<uint32_t> Background = {0, 1, 2},
                        std::vector<uint64_t> ThresholdTime = {0, 1, 2},
                        std::vector<uint64_t> PeakTime = {0, 1, 2})
      : Amplitude(std::move(Amplitude)), PeakArea(std::move(PeakArea)),
        Background(std::move(Background)),
        ThresholdTime(std::move(ThresholdTime)),
        PeakTime(std::move(PeakTime)){};

  std::vector<uint32_t> const Amplitude;
  std::vector<uint32_t> const PeakArea;
  std::vector<uint32_t> const Background;
  std::vector<uint64_t> const ThresholdTime;
  std::vector<uint64_t> const PeakTime;
};

static flatbuffers::DetachedBuffer
generateFlatbufferData(std::string const &SourceName = "TestSource",
                       uint64_t const MessageID = 0,
                       uint64_t const PulseTime = 1,
                       std::vector<uint32_t> const &TimeOfFlight = {0, 1, 2},
                       std::vector<uint32_t> const &DetectorID = {0, 1, 2},
                       bool IncludeAdcDebugInfo = false,
                       AdcDebugInfo const &AdcDebugData = AdcDebugInfo()) {
  flatbuffers::FlatBufferBuilder builder;

  auto FBSourceNameOffset = builder.CreateString(SourceName);
  auto FBTimeOfFlightOffset = builder.CreateVector(TimeOfFlight);
  auto FBDetectorIDOffset = builder.CreateVector(DetectorID);

  auto FBAmplitudeOffset = builder.CreateVector(AdcDebugData.Amplitude);
  auto FBPeakAreaOffset = builder.CreateVector(AdcDebugData.PeakArea);
  auto FBBackgroundOffset = builder.CreateVector(AdcDebugData.Background);
  auto FBThresholdTimeOffset = builder.CreateVector(AdcDebugData.ThresholdTime);
  auto FBPeakTimeOffset = builder.CreateVector(AdcDebugData.PeakTime);
  auto FBAdcInfoOffset = CreateAdcPulseDebug(
      builder, FBAmplitudeOffset, FBPeakAreaOffset, FBBackgroundOffset,
      FBThresholdTimeOffset, FBPeakTimeOffset);

  EventMessageBuilder MessageBuilder(builder);
  MessageBuilder.add_source_name(FBSourceNameOffset);
  MessageBuilder.add_message_id(MessageID);
  MessageBuilder.add_pulse_time(PulseTime);
  MessageBuilder.add_time_of_flight(FBTimeOfFlightOffset);
  MessageBuilder.add_detector_id(FBDetectorIDOffset);

  if (IncludeAdcDebugInfo) {
    MessageBuilder.add_facility_specific_data_type(FacilityData::AdcPulseDebug);
    MessageBuilder.add_facility_specific_data(FBAdcInfoOffset.Union());
  }

  builder.Finish(MessageBuilder.Finish(), EventMessageIdentifier());

  // Note, Release gives us a "DetachedBuffer" which owns the data
  return builder.Release();
}

/// Modifies the input vector such that it is the concatenation of itself with
/// itself
template <typename T> void repeatVector(std::vector<T> &InputVector) {
  InputVector.insert(InputVector.end(), InputVector.begin(), InputVector.end());
}

class EventReaderTests : public ::testing::Test {
public:
  void SetUp() override {
    ReaderUnderTest = std::make_unique<AccessMessageMetadata::ev42_Extractor>();
    setExtractorModule<AccessMessageMetadata::ev42_Extractor>("ev42");
  };

  std::unique_ptr<AccessMessageMetadata::ev42_Extractor> ReaderUnderTest;
};

TEST_F(EventReaderTests, ReaderReturnsSourceNameFromMessage) {
  std::string const TestSourceName = "TestSource";
  auto MessageBuffer = generateFlatbufferData(TestSourceName);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());
  EXPECT_EQ(ReaderUnderTest->source_name(TestMessage), TestSourceName);
}

TEST_F(EventReaderTests, ReaderReturnsPulseTimeAsMessageTimestamp) {
  uint64_t PulseTime = 42;
  auto MessageBuffer = generateFlatbufferData("TestSource", 0, PulseTime);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());
  EXPECT_EQ(ReaderUnderTest->timestamp(TestMessage), PulseTime);
}

TEST_F(EventReaderTests, ReaderVerifiesValidMessage) {
  auto MessageBuffer = generateFlatbufferData();
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());
  EXPECT_TRUE(ReaderUnderTest->verify(TestMessage));
}
