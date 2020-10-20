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
#include "WriterModule/ev42/ev42_Writer.h"
#include "helpers/HDFFileTestHelper.h"
#include "helpers/SetExtractorModule.h"

using namespace WriterModule::ev42;

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

flatbuffers::DetachedBuffer
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

AdcDebugInfo readAdcPulseDataFromFile(hdf5::node::Group &TestGroup) {
  auto AmplitudeDataset = TestGroup.get_dataset("adc_pulse_amplitude");
  auto PeakAreaDataset = TestGroup.get_dataset("adc_pulse_peak_area");
  auto BackgroundDataset = TestGroup.get_dataset("adc_pulse_background");
  auto ThresholdTimeDataset = TestGroup.get_dataset("adc_pulse_threshold_time");
  auto PeakTimeDataset = TestGroup.get_dataset("adc_pulse_peak_time");
  std::vector<uint32_t> AmplitudeFromFile(AmplitudeDataset.dataspace().size());
  std::vector<uint32_t> PeakAreaFromFile(PeakAreaDataset.dataspace().size());
  std::vector<uint32_t> BackgroundFromFile(
      BackgroundDataset.dataspace().size());
  std::vector<uint64_t> ThresholdTimeFromFile(
      ThresholdTimeDataset.dataspace().size());
  std::vector<uint64_t> PeakTimeFromFile(PeakTimeDataset.dataspace().size());
  AmplitudeDataset.read(AmplitudeFromFile);
  PeakAreaDataset.read(PeakAreaFromFile);
  BackgroundDataset.read(BackgroundFromFile);
  ThresholdTimeDataset.read(ThresholdTimeFromFile);
  PeakTimeDataset.read(PeakTimeFromFile);

  return AdcDebugInfo(AmplitudeFromFile, PeakAreaFromFile, BackgroundFromFile,
                      ThresholdTimeFromFile, PeakTimeFromFile);
}

class EventWriterTests : public ::testing::Test {
public:
  void SetUp() override {
    File = HDFFileTestHelper::createInMemoryTestFile("EventWriterTestFile.nxs");
    TestGroup = File->hdfGroup().create_group(TestGroupName);
    setExtractorModule<AccessMessageMetadata::ev42_Extractor>("ev42");
  };

  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> File;
  hdf5::node::Group TestGroup;
  std::string const TestGroupName = "test_group";
};

using WriterModule::InitResult;

TEST_F(EventWriterTests, WriterInitialisesFileWithNXEventDataDatasets) {
  {
    WriterModule::ev42::ev42_Writer Writer;
    Writer.parse_config("{}");
    EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
  }
  ASSERT_TRUE(File->hdfGroup().has_group(TestGroupName));
  EXPECT_TRUE(TestGroup.has_dataset("event_time_offset"));
  EXPECT_TRUE(TestGroup.has_dataset("event_time_zero"));
  EXPECT_TRUE(TestGroup.has_dataset("event_index"));
  EXPECT_TRUE(TestGroup.has_dataset("event_id"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_index"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_timestamp_zero"));

  // Did not specify adc_pulse_debug in configuration so no ADC datasets should
  // be created
  EXPECT_FALSE(TestGroup.has_dataset("adc_pulse_amplitude"));
  EXPECT_FALSE(TestGroup.has_dataset("adc_pulse_peak_area"));
  EXPECT_FALSE(TestGroup.has_dataset("adc_pulse_background"));
  EXPECT_FALSE(TestGroup.has_dataset("adc_pulse_threshold_time"));
  EXPECT_FALSE(TestGroup.has_dataset("adc_pulse_peak_time"));
}

TEST_F(EventWriterTests, WriterCreatesUnitsAttributesForTimeDatasets) {
  {
    WriterModule::ev42::ev42_Writer Writer;
    Writer.parse_config("{}");
    EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
  }
  ASSERT_TRUE(File->hdfGroup().has_group(TestGroupName));
  EXPECT_TRUE(TestGroup.has_dataset("event_time_offset"));
  EXPECT_TRUE(TestGroup.has_dataset("event_time_zero"));

  std::string const expected_time_units = "ns";

  std::string time_zero_units;
  EXPECT_NO_THROW(
      TestGroup["event_time_zero"].attributes["units"].read(time_zero_units))
      << "Expect units attribute to be present on the event_time_zero dataset";
  EXPECT_EQ(time_zero_units, expected_time_units)
      << fmt::format("Expect time units to be {}", expected_time_units);

  std::string time_offset_units;
  EXPECT_NO_THROW(TestGroup["event_time_offset"].attributes["units"].read(
      time_offset_units))
      << "Expect units attribute to be present on the event_time_zero dataset";
  EXPECT_EQ(time_offset_units, expected_time_units)
      << fmt::format("Expect time units to be {}", expected_time_units);
}

TEST_F(
    EventWriterTests,
    WriterInitialisesFileWithNXEventDataDatasetsAndAdcDatasetsWhenRequested) {
  {
    WriterModule::ev42::ev42_Writer Writer;
    // Tell writer module to write ADC pulse debug data
    Writer.parse_config("{\"adc_pulse_debug\": true}");
    EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
  }
  ASSERT_TRUE(File->hdfGroup().has_group(TestGroupName));
  EXPECT_TRUE(TestGroup.has_dataset("event_time_offset"));
  EXPECT_TRUE(TestGroup.has_dataset("event_time_zero"));
  EXPECT_TRUE(TestGroup.has_dataset("event_index"));
  EXPECT_TRUE(TestGroup.has_dataset("event_id"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_index"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_timestamp_zero"));

  // Specified adc_pulse_debug in configuration so ADC datasets should be
  // created
  EXPECT_TRUE(TestGroup.has_dataset("adc_pulse_amplitude"));
  EXPECT_TRUE(TestGroup.has_dataset("adc_pulse_peak_area"));
  EXPECT_TRUE(TestGroup.has_dataset("adc_pulse_background"));
  EXPECT_TRUE(TestGroup.has_dataset("adc_pulse_threshold_time"));
  EXPECT_TRUE(TestGroup.has_dataset("adc_pulse_peak_time"));
}

TEST_F(EventWriterTests, WriterFailsToReopenGroupWhichWasNeverInitialised) {
  WriterModule::ev42::ev42_Writer Writer;
  EXPECT_FALSE(Writer.reopen(TestGroup) == InitResult::OK);
}

TEST_F(EventWriterTests, WriterSuccessfullyReopensGroupWhichWasInitialised) {
  WriterModule::ev42::ev42_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
}

TEST_F(EventWriterTests, WriterReportsFailureIfTryToInitialiseTwice) {
  WriterModule::ev42::ev42_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
}

TEST_F(EventWriterTests, WriterSuccessfullyRecordsEventDataFromSingleMessage) {
  // Create a single event message with data we can later check is recorded in
  // the file
  uint64_t const PulseTime = 42;
  std::vector<uint32_t> const TimeOfFlight = {0, 1, 2};
  std::vector<uint32_t> const DetectorID = {3, 4, 5};
  auto MessageBuffer = generateFlatbufferData("TestSource", 0, PulseTime,
                                              TimeOfFlight, DetectorID);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev42::ev42_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage));
  } // These braces are required due to "h5.cpp"

  // Read data from the file
  auto EventTimeOffsetDataset = TestGroup.get_dataset("event_time_offset");
  auto EventTimeZeroDataset = TestGroup.get_dataset("event_time_zero");
  auto EventIndexDataset = TestGroup.get_dataset("event_index");
  auto EventIDDataset = TestGroup.get_dataset("event_id");
  std::vector<uint32_t> EventTimeOffset(
      EventTimeOffsetDataset.dataspace().size());
  std::vector<uint64_t> EventTimeZero(EventTimeZeroDataset.dataspace().size());
  std::vector<uint32_t> EventIndex(EventIndexDataset.dataspace().size());
  std::vector<uint32_t> EventID(EventIDDataset.dataspace().size());
  EventTimeOffsetDataset.read(EventTimeOffset);
  EventTimeZeroDataset.read(EventTimeZero);
  EventIndexDataset.read(EventIndex);
  EventIDDataset.read(EventID);

  // Test data in file matches what we originally put in the message
  EXPECT_THAT(EventTimeOffset, testing::ContainerEq(TimeOfFlight))
      << "Expected event_time_offset dataset to contain the time of flight "
         "values from the message";
  EXPECT_EQ(EventTimeZero.size(), 1U)
      << "Expected event_time_zero to contain a "
         "single value, as we wrote a single "
         "message";
  EXPECT_EQ(EventTimeZero[0], PulseTime)
      << "Expected event_time_zero to contain the pulse time from the message";
  EXPECT_EQ(EventIndex.size(), 1U)
      << "Expected event_index to contain a single "
         "value, as we wrote a single message";
  EXPECT_EQ(EventIndex[0], 0U)
      << "Expected single event_index value to be zero "
         "as we wrote only one message";
  EXPECT_THAT(EventID, testing::ContainerEq(DetectorID))
      << "Expected event_id dataset to contain the detector ID "
         "values from the message";
}

TEST_F(EventWriterTests, WriterSuccessfullyRecordsEventDataFromTwoMessages) {
  // Create a single event message with data we can later check is recorded in
  // the file
  uint64_t const PulseTime = 42;
  std::vector<uint32_t> TimeOfFlight = {0, 1, 2};
  std::vector<uint32_t> DetectorID = {3, 4, 5};
  auto MessageBuffer = generateFlatbufferData("TestSource", 0, PulseTime,
                                              TimeOfFlight, DetectorID);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev42::ev42_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage));
    EXPECT_NO_THROW(Writer.write(TestMessage));
  } // These braces are required due to "h5.cpp"

  // Read data from the file
  auto EventTimeOffsetDataset = TestGroup.get_dataset("event_time_offset");
  auto EventTimeZeroDataset = TestGroup.get_dataset("event_time_zero");
  auto EventIndexDataset = TestGroup.get_dataset("event_index");
  auto EventIDDataset = TestGroup.get_dataset("event_id");
  std::vector<uint32_t> EventTimeOffset(
      EventTimeOffsetDataset.dataspace().size());
  std::vector<uint64_t> EventTimeZero(EventTimeZeroDataset.dataspace().size());
  std::vector<uint32_t> EventIndex(EventIndexDataset.dataspace().size());
  std::vector<uint32_t> EventID(EventIDDataset.dataspace().size());
  EventTimeOffsetDataset.read(EventTimeOffset);
  EventTimeZeroDataset.read(EventTimeZero);
  EventIndexDataset.read(EventIndex);
  EventIDDataset.read(EventID);

  // Repeat the input value vectors as the same message should be written twice
  repeatVector(TimeOfFlight);
  repeatVector(DetectorID);

  // Test data in file matches what we originally put in the message
  EXPECT_THAT(EventTimeOffset, testing::ContainerEq(TimeOfFlight))
      << "Expected event_time_offset dataset to contain the time of flight "
         "values from both messages";
  EXPECT_EQ(EventTimeZero.size(), 2U)
      << "Expected event_time_zero to contain a "
         "two values, as we wrote two "
         "messages";
  EXPECT_EQ(EventTimeZero[0], PulseTime)
      << "Expected event_time_zero to contain the pulse time from the message";
  EXPECT_EQ(EventIndex.size(), 2U) << "Expected event_index to contain two "
                                      "values, as we wrote two messages";
  EXPECT_EQ(EventIndex[1], 3U) << "Expected second message to start at index 3 "
                                  "as there were 3 events in the first message";
  EXPECT_THAT(EventID, testing::ContainerEq(DetectorID))
      << "Expected event_id dataset to contain the detector ID "
         "values from both messages";
}

TEST_F(EventWriterTests,
       WriterSuccessfullyRecordsAdcPulseDebugDataWhenPresentInSingleMessage) {
  // Create a single event message with Adc data we can later check is recorded
  // in the file
  std::vector<uint32_t> const Amplitude = {0, 1, 2};
  std::vector<uint32_t> const PeakArea = {3, 4, 5};
  std::vector<uint32_t> const Background = {6, 7, 8};
  std::vector<uint64_t> const ThresholdTime = {9, 10, 11};
  std::vector<uint64_t> const PeakTime = {12, 13, 14};
  auto AdcDebugData =
      AdcDebugInfo(Amplitude, PeakArea, Background, ThresholdTime, PeakTime);
  auto MessageBuffer = generateFlatbufferData("TestSource", 0, 1, {0, 0, 0},
                                              {0, 0, 0}, true, AdcDebugData);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev42::ev42_Writer Writer;
    // Tell writer module to write ADC pulse debug data
    Writer.parse_config("{\"adc_pulse_debug\": true}");
    EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage));
  } // These braces are required due to "h5.cpp"

  // Test data in file matches what we originally put in the message
  auto AdcInfoFromFile = readAdcPulseDataFromFile(TestGroup);
  EXPECT_THAT(AdcInfoFromFile.Amplitude, testing::ContainerEq(Amplitude));
  EXPECT_THAT(AdcInfoFromFile.PeakArea, testing::ContainerEq(PeakArea));
  EXPECT_THAT(AdcInfoFromFile.Background, testing::ContainerEq(Background));
  EXPECT_THAT(AdcInfoFromFile.ThresholdTime,
              testing::ContainerEq(ThresholdTime));
  EXPECT_THAT(AdcInfoFromFile.PeakTime, testing::ContainerEq(PeakTime));
}

TEST_F(EventWriterTests,
       WriterSuccessfullyRecordsAdcPulseDebugDataWhenPresentInTwoMessages) {
  // Create a single event message with Adc data we can later check is recorded
  // in the file
  std::vector<uint32_t> Amplitude = {0, 1, 2};
  std::vector<uint32_t> PeakArea = {3, 4, 5};
  std::vector<uint32_t> Background = {6, 7, 8};
  std::vector<uint64_t> ThresholdTime = {9, 10, 11};
  std::vector<uint64_t> PeakTime = {12, 13, 14};
  auto AdcDebugData =
      AdcDebugInfo(Amplitude, PeakArea, Background, ThresholdTime, PeakTime);
  auto MessageBuffer = generateFlatbufferData("TestSource", 0, 1, {0, 0, 0},
                                              {0, 0, 0}, true, AdcDebugData);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev42::ev42_Writer Writer;
    // Tell writer module to write ADC pulse debug data
    Writer.parse_config("{\"adc_pulse_debug\": true}");
    EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage)); // First message
    EXPECT_NO_THROW(Writer.write(TestMessage)); // Second message
  } // These braces are required due to "h5.cpp"

  // Repeat the input value vectors as the same message should be written twice
  repeatVector(Amplitude);
  repeatVector(PeakArea);
  repeatVector(Background);
  repeatVector(ThresholdTime);
  repeatVector(PeakTime);

  // Test data in file matches what we originally put in the messages
  auto AdcInfoFromFile = readAdcPulseDataFromFile(TestGroup);
  EXPECT_THAT(AdcInfoFromFile.Amplitude, testing::ContainerEq(Amplitude));
  EXPECT_THAT(AdcInfoFromFile.PeakArea, testing::ContainerEq(PeakArea));
  EXPECT_THAT(AdcInfoFromFile.Background, testing::ContainerEq(Background));
  EXPECT_THAT(AdcInfoFromFile.ThresholdTime,
              testing::ContainerEq(ThresholdTime));
  EXPECT_THAT(AdcInfoFromFile.PeakTime, testing::ContainerEq(PeakTime));
}

TEST_F(EventWriterTests,
       WriterRecordsZeroesDataWhenAdcPulseDebugMissingFromMessage) {
  // So that the ADC pulse datasets are still consistent with event_index and
  // event_time_zero, the writer should record zeroes if a message arrives
  // without ADC pulse data

  std::vector<uint32_t> Amplitude = {0, 1, 2};
  std::vector<uint32_t> PeakArea = {3, 4, 5};
  std::vector<uint32_t> Background = {6, 7, 8};
  std::vector<uint64_t> ThresholdTime = {9, 10, 11};
  std::vector<uint64_t> PeakTime = {12, 13, 14};
  auto AdcDebugData =
      AdcDebugInfo(Amplitude, PeakArea, Background, ThresholdTime, PeakTime);

  // First message with ADC pulse data
  auto MessageBuffer = generateFlatbufferData("TestSource", 0, 1, {0, 0, 0},
                                              {0, 0, 0}, true, AdcDebugData);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());

  // Second message without ADC pulse data
  auto SecondMessageBuffer =
      generateFlatbufferData("TestSource", 0, 1, {0, 0, 0}, {0, 0, 0}, false);
  FileWriter::FlatbufferMessage SecondTestMessage(SecondMessageBuffer.data(),
                                                  SecondMessageBuffer.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev42::ev42_Writer Writer;
    // Tell writer module to write ADC pulse debug data
    Writer.parse_config("{\"adc_pulse_debug\": true}");
    EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage));       // First message
    EXPECT_NO_THROW(Writer.write(SecondTestMessage)); // Second message
  } // These braces are required due to "h5.cpp"

  // Append zeroes to the ADC data from the first message, equivalent to the
  // number of events in the second message
  size_t NumberOfEventsInSecondMessage = 3;
  Amplitude.resize(Amplitude.size() + NumberOfEventsInSecondMessage, 0);
  PeakArea.resize(PeakArea.size() + NumberOfEventsInSecondMessage, 0);
  Background.resize(Background.size() + NumberOfEventsInSecondMessage, 0);
  ThresholdTime.resize(ThresholdTime.size() + NumberOfEventsInSecondMessage, 0);
  PeakTime.resize(PeakTime.size() + NumberOfEventsInSecondMessage, 0);

  // Test data in file matches what we originally put in the first message, plus
  // the expected zeroes
  auto AdcInfoFromFile = readAdcPulseDataFromFile(TestGroup);
  EXPECT_THAT(AdcInfoFromFile.Amplitude, testing::ContainerEq(Amplitude));
  EXPECT_THAT(AdcInfoFromFile.PeakArea, testing::ContainerEq(PeakArea));
  EXPECT_THAT(AdcInfoFromFile.Background, testing::ContainerEq(Background));
  EXPECT_THAT(AdcInfoFromFile.ThresholdTime,
              testing::ContainerEq(ThresholdTime));
  EXPECT_THAT(AdcInfoFromFile.PeakTime, testing::ContainerEq(PeakTime));
}
