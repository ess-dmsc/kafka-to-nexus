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

#include <ev44_events_generated.h>
#include <gmock/gmock.h>

#include <utility>

#include "AccessMessageMetadata/ev44/ev44_Extractor.h"
#include "WriterModule/ev44/ev44_Writer.h"
#include "helpers/HDFFileTestHelper.h"
#include "helpers/SetExtractorModule.h"

using namespace WriterModule::ev44;

flatbuffers::DetachedBuffer generateFlatbufferData(
    std::string const &SourceName = "TestSource", uint64_t const MessageID = 0,
    std::vector<int32_t> const &TimeOfFlight = {101, 102, 201},
    std::vector<int32_t> const &DetectorID = {101, 102, 201},
    std::vector<int64_t> const &ReferenceTime = {1000, 2000},
    std::vector<int32_t> const &ReferenceTimeIndex = {0, 2}) {
  flatbuffers::FlatBufferBuilder builder;

  auto FBSourceNameOffset = builder.CreateString(SourceName);
  auto FBTimeOfFlightOffset = builder.CreateVector(TimeOfFlight);
  auto FBDetectorIDOffset = builder.CreateVector(DetectorID);
  auto FBReferenceTime = builder.CreateVector(ReferenceTime);
  auto FBReferenceTimeIndex = builder.CreateVector(ReferenceTimeIndex);

  Event44MessageBuilder MessageBuilder(builder);
  MessageBuilder.add_source_name(FBSourceNameOffset);
  MessageBuilder.add_message_id(MessageID);
  MessageBuilder.add_time_of_flight(FBTimeOfFlightOffset);
  MessageBuilder.add_pixel_id(FBDetectorIDOffset);
  MessageBuilder.add_reference_time(FBReferenceTime);
  MessageBuilder.add_reference_time_index(FBReferenceTimeIndex);

  builder.Finish(MessageBuilder.Finish(), Event44MessageIdentifier());

  // Note, Release gives us a "DetachedBuffer" which owns the data
  return builder.Release();
}

template <typename T>
std::vector<T> concatenateVectors(const std::vector<T> &Vector1,
                                  const std::vector<T> &Vector2) {
  std::vector<T> ConcatenatedVector;
  ConcatenatedVector.reserve(Vector1.size() +
                             Vector2.size()); // Pre-allocate memory
  ConcatenatedVector.insert(ConcatenatedVector.end(), Vector1.begin(),
                            Vector1.end());
  ConcatenatedVector.insert(ConcatenatedVector.end(), Vector2.begin(),
                            Vector2.end());
  return ConcatenatedVector;
}

class Event44WriterTests : public ::testing::Test {
public:
  void SetUp() override {
    File =
        HDFFileTestHelper::createInMemoryTestFile("Event44WriterTestFile.nxs");
    RootGroup = File->hdfGroup();
    TestGroup = RootGroup.create_group(TestGroupName);
    setExtractorModule<AccessMessageMetadata::ev44_Extractor>("ev44");
  };

  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> File;
  hdf5::node::Group TestGroup;
  std::string const TestGroupName = "test_group";
  hdf5::node::Group RootGroup;
};

using WriterModule::InitResult;

TEST_F(Event44WriterTests, WriterInitialisesFileWithNXEventDataDatasets) {
  {
    WriterModule::ev44::ev44_Writer Writer;
    Writer.parse_config("{}");
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
  }
  ASSERT_TRUE(File->hdfGroup().has_group(TestGroupName));
  EXPECT_TRUE(TestGroup.has_dataset("event_time_offset"));
  EXPECT_TRUE(TestGroup.has_dataset("event_time_zero"));
  EXPECT_TRUE(TestGroup.has_dataset("event_index"));
  EXPECT_TRUE(TestGroup.has_dataset("event_id"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_index"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_timestamp_zero"));
}

TEST_F(Event44WriterTests, WriterCreatesUnitsAttributesForTimeDatasets) {
  {
    WriterModule::ev44::ev44_Writer Writer;
    Writer.parse_config("{}");
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
  }
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

  std::string cue_timestamp_zero_units;
  EXPECT_NO_THROW(TestGroup["cue_timestamp_zero"].attributes["units"].read(
      cue_timestamp_zero_units))
      << "Expect units attribute to be present on the cue_timestamp_zero "
         "dataset";
  EXPECT_EQ(cue_timestamp_zero_units, expected_time_units)
      << fmt::format("Expect time units to be {}", expected_time_units);
}

TEST_F(Event44WriterTests, WriterFailsToReopenGroupWhichWasNeverInitialised) {
  WriterModule::ev44::ev44_Writer Writer;
  EXPECT_FALSE(Writer.reopen(TestGroup) == InitResult::OK);
}

TEST_F(Event44WriterTests, WriterSuccessfullyReopensGroupWhichWasInitialised) {
  WriterModule::ev44::ev44_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
}

TEST_F(Event44WriterTests, WriterReportsFailureIfTryToInitialiseTwice) {
  WriterModule::ev44::ev44_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(TestGroup) == InitResult::OK);
}

TEST_F(Event44WriterTests,
       WriterSuccessfullyRecordsEventDataFromSingleMessage) {
  // Create a single event message with data we can later check is recorded in
  // the file
  std::vector<int32_t> const TimeOfFlight = {101, 102, 201};
  std::vector<int32_t> const DetectorID = {101, 102, 201};
  std::vector<int64_t> const ReferenceTime = {1000, 2000};
  std::vector<int32_t> const ReferenceTimeIndex = {0, 2};
  auto MessageBuffer =
      generateFlatbufferData("TestSource", 0, TimeOfFlight, DetectorID,
                             ReferenceTime, ReferenceTimeIndex);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev44::ev44_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage, false));
  } // These braces are required due to "h5.cpp"

  // Read data from the file
  auto EventTimeOffsetDataset = TestGroup.get_dataset("event_time_offset");
  auto EventTimeZeroDataset = TestGroup.get_dataset("event_time_zero");
  auto EventIndexDataset = TestGroup.get_dataset("event_index");
  auto EventIDDataset = TestGroup.get_dataset("event_id");
  std::vector<int32_t> EventTimeOffset(
      EventTimeOffsetDataset.dataspace().size());
  std::vector<int64_t> EventTimeZero(EventTimeZeroDataset.dataspace().size());
  std::vector<int32_t> EventIndex(EventIndexDataset.dataspace().size());
  std::vector<int32_t> EventID(EventIDDataset.dataspace().size());
  EventTimeOffsetDataset.read(EventTimeOffset);
  EventTimeZeroDataset.read(EventTimeZero);
  EventIndexDataset.read(EventIndex);
  EventIDDataset.read(EventID);

  // Test data in file matches what we originally put in the message
  EXPECT_THAT(EventTimeOffset, testing::ContainerEq(TimeOfFlight))
      << "Expected event_time_offset dataset to contain the time of flight "
         "values from the message";
  EXPECT_THAT(EventTimeZero, testing::ContainerEq(ReferenceTime))
      << "Expected event_time_zero dataset to contain the pulse time "
         "values from the message";
  EXPECT_THAT(EventIndex, testing::ContainerEq(ReferenceTimeIndex))
      << "Expected event_index dataset to contain the index of first event for "
         "the corresponding pulse ";
  EXPECT_EQ(EventIndex[0], 0)
      << "Expected first event_index value to be zero "
         "as the first pulse is always about the first message";
  EXPECT_THAT(EventID, testing::ContainerEq(DetectorID))
      << "Expected event_id dataset to contain the detector ID "
         "values from the message";
}

TEST_F(Event44WriterTests, WriterSuccessfullyRecordsEventDataWithoutPixelIds) {
  // in ev44 time_of_flight is mandatory if events are present, but pixel_id
  // can be empty
  std::vector<int32_t> const TimeOfFlight = {101, 102, 201};
  std::vector<int32_t> const DetectorID = {};
  std::vector<int64_t> const ReferenceTime = {1000, 2000};
  std::vector<int32_t> const ReferenceTimeIndex = {0, 2};
  auto MessageBuffer =
      generateFlatbufferData("TestSource", 0, TimeOfFlight, DetectorID,
                             ReferenceTime, ReferenceTimeIndex);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev44::ev44_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage, false));
  } // These braces are required due to "h5.cpp"

  // Read data from the file
  auto EventTimeOffsetDataset = TestGroup.get_dataset("event_time_offset");
  auto EventTimeZeroDataset = TestGroup.get_dataset("event_time_zero");
  auto EventIndexDataset = TestGroup.get_dataset("event_index");
  auto EventIDDataset = TestGroup.get_dataset("event_id");
  std::vector<int32_t> EventTimeOffset(
      EventTimeOffsetDataset.dataspace().size());
  std::vector<int64_t> EventTimeZero(EventTimeZeroDataset.dataspace().size());
  std::vector<int32_t> EventIndex(EventIndexDataset.dataspace().size());
  std::vector<int32_t> EventID(EventIDDataset.dataspace().size());
  EventTimeOffsetDataset.read(EventTimeOffset);
  EventTimeZeroDataset.read(EventTimeZero);
  EventIndexDataset.read(EventIndex);
  EventIDDataset.read(EventID);

  // Test data in file matches what we originally put in the message
  EXPECT_THAT(EventTimeOffset, testing::ContainerEq(TimeOfFlight))
      << "Expected event_time_offset dataset to contain the time of flight "
         "values from the message";
  EXPECT_THAT(EventTimeZero, testing::ContainerEq(ReferenceTime))
      << "Expected event_time_zero dataset to contain the pulse time "
         "values from the message";
  EXPECT_THAT(EventIndex, testing::ContainerEq(ReferenceTimeIndex))
      << "Expected event_index dataset to contain the index of first event for "
         "the corresponding pulse ";
  EXPECT_EQ(EventIndex[0], 0)
      << "Expected first event_index value to be zero "
         "as the first pulse is always about the first message";
  EXPECT_THAT(EventID, testing::ContainerEq(DetectorID))
      << "Expected event_id dataset to contain the detector ID "
         "values from the message";
}

TEST_F(Event44WriterTests, WriterSuccessfullyRecordsEventDataFromTwoMessages) {
  // Create a single event message with data we can later check is recorded in
  // the file
  std::vector<int32_t> TimeOfFlight1 = {101, 102, 201};
  std::vector<int32_t> TimeOfFlight2 = {301, 302, 303, 401, 501, 502};
  std::vector<int32_t> DetectorID1 = {101, 102, 201};
  std::vector<int32_t> DetectorID2 = {301, 302, 303, 401, 501, 502};
  std::vector<int64_t> ReferenceTime1 = {1000, 2000};
  std::vector<int64_t> ReferenceTime2 = {3000, 4000, 5000};
  std::vector<int32_t> ReferenceTimeIndex1 = {0, 2};
  std::vector<int32_t> ReferenceTimeIndex2 = {0, 3, 4};
  auto MessageBuffer1 =
      generateFlatbufferData("TestSource", 1, TimeOfFlight1, DetectorID1,
                             ReferenceTime1, ReferenceTimeIndex1);
  FileWriter::FlatbufferMessage TestMessage1(MessageBuffer1.data(),
                                             MessageBuffer1.size());
  auto MessageBuffer2 =
      generateFlatbufferData("TestSource", 2, TimeOfFlight2, DetectorID2,
                             ReferenceTime2, ReferenceTimeIndex2);
  FileWriter::FlatbufferMessage TestMessage2(MessageBuffer2.data(),
                                             MessageBuffer2.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev44::ev44_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage1, false));
    EXPECT_NO_THROW(Writer.write(TestMessage2, false));
  } // These braces are required due to "h5.cpp"

  // Read data from the file
  auto EventTimeOffsetDataset = TestGroup.get_dataset("event_time_offset");
  auto EventTimeZeroDataset = TestGroup.get_dataset("event_time_zero");
  auto EventIndexDataset = TestGroup.get_dataset("event_index");
  auto EventIDDataset = TestGroup.get_dataset("event_id");
  std::vector<int32_t> EventTimeOffset(
      EventTimeOffsetDataset.dataspace().size());
  std::vector<int64_t> EventTimeZero(EventTimeZeroDataset.dataspace().size());
  std::vector<int32_t> EventIndex(EventIndexDataset.dataspace().size());
  std::vector<int32_t> EventID(EventIDDataset.dataspace().size());
  EventTimeOffsetDataset.read(EventTimeOffset);
  EventTimeZeroDataset.read(EventTimeZero);
  EventIndexDataset.read(EventIndex);
  EventIDDataset.read(EventID);

  // Duplicate events
  std::vector<int32_t> ExpectedTimeOfFlight =
      concatenateVectors(TimeOfFlight1, TimeOfFlight2);
  std::vector<int32_t> ExpectedDetectorID =
      concatenateVectors(DetectorID1, DetectorID2);
  std::vector<int64_t> ExpectedReferenceTime =
      concatenateVectors(ReferenceTime1, ReferenceTime2);
  std::vector<int32_t> ExpectedReferenceTimeIndex;
  int32_t numberOfEventsInFirstMessage =
      static_cast<int32_t>(DetectorID1.size());
  for (int32_t value : ReferenceTimeIndex1) {
    ExpectedReferenceTimeIndex.push_back(value);
  }
  for (int32_t value : ReferenceTimeIndex2) {
    ExpectedReferenceTimeIndex.push_back(value + numberOfEventsInFirstMessage);
  }

  // Test data in file matches what we originally put in the message
  EXPECT_THAT(EventTimeOffset, testing::ContainerEq(ExpectedTimeOfFlight))
      << "Expected event_time_offset dataset to contain the time of flight "
         "values from both messages";
  EXPECT_THAT(EventTimeZero, testing::ContainerEq(ExpectedReferenceTime))
      << "Expected event_time_zero dataset to contain the pulse time "
         "values from the message";
  EXPECT_THAT(EventIndex, testing::ContainerEq(ExpectedReferenceTimeIndex))
      << "Expected event_index dataset to contain the index of first event for "
         "the corresponding pulse ";
  EXPECT_EQ(EventIndex[ReferenceTime1.size()], numberOfEventsInFirstMessage)
      << "Expected the firts event_index for the second message to start at an "
         "index "
         "equal to the number of events in the first message ";
  EXPECT_THAT(EventID, testing::ContainerEq(ExpectedDetectorID))
      << "Expected event_id dataset to contain the detector ID "
         "values from both messages";
}

TEST_F(Event44WriterTests, WriterSuccessfullyHandlesMessageWithNoEvents) {
  std::vector<int32_t> TimeOfFlight1 = {101, 102, 201};
  std::vector<int32_t> TimeOfFlight2 = {}; // no events in this message
  std::vector<int32_t> TimeOfFlight3 = {301, 302, 303, 401, 501, 502};
  std::vector<int32_t> DetectorID1 = {101, 102, 201};
  std::vector<int32_t> DetectorID2 = {}; // no events in this message
  std::vector<int32_t> DetectorID3 = {301, 302, 303, 401, 501, 502};
  std::vector<int64_t> ReferenceTime1 = {1000, 2000};
  std::vector<int64_t> ReferenceTime2 = {2500}; // pulse time is present
  std::vector<int64_t> ReferenceTime3 = {3000, 4000, 5000};
  std::vector<int32_t> ReferenceTimeIndex1 = {0, 2};
  std::vector<int32_t> ReferenceTimeIndex2 = {-1};
  std::vector<int32_t> ReferenceTimeIndex3 = {0, 3, 4};
  auto MessageBuffer1 =
      generateFlatbufferData("TestSource", 1, TimeOfFlight1, DetectorID1,
                             ReferenceTime1, ReferenceTimeIndex1);
  FileWriter::FlatbufferMessage TestMessage1(MessageBuffer1.data(),
                                             MessageBuffer1.size());
  auto MessageBuffer2 =
      generateFlatbufferData("TestSource", 2, TimeOfFlight2, DetectorID2,
                             ReferenceTime2, ReferenceTimeIndex2);
  FileWriter::FlatbufferMessage TestMessage2(MessageBuffer2.data(),
                                             MessageBuffer2.size());
  auto MessageBuffer3 =
      generateFlatbufferData("TestSource", 3, TimeOfFlight3, DetectorID3,
                             ReferenceTime3, ReferenceTimeIndex3);
  FileWriter::FlatbufferMessage TestMessage3(MessageBuffer3.data(),
                                             MessageBuffer3.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev44::ev44_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage1, false));
    EXPECT_NO_THROW(Writer.write(TestMessage2, false));
    EXPECT_NO_THROW(Writer.write(TestMessage3, false));
  } // These braces are required due to "h5.cpp"

  // Read data from the file
  auto EventTimeOffsetDataset = TestGroup.get_dataset("event_time_offset");
  auto EventTimeZeroDataset = TestGroup.get_dataset("event_time_zero");
  auto EventIndexDataset = TestGroup.get_dataset("event_index");
  auto EventIDDataset = TestGroup.get_dataset("event_id");
  std::vector<int32_t> EventTimeOffset(
      EventTimeOffsetDataset.dataspace().size());
  std::vector<int64_t> EventTimeZero(EventTimeZeroDataset.dataspace().size());
  std::vector<int32_t> EventIndex(EventIndexDataset.dataspace().size());
  std::vector<int32_t> EventID(EventIDDataset.dataspace().size());
  EventTimeOffsetDataset.read(EventTimeOffset);
  EventTimeZeroDataset.read(EventTimeZero);
  EventIndexDataset.read(EventIndex);
  EventIDDataset.read(EventID);

  // Duplicate events
  std::vector<int32_t> ExpectedTimeOfFlight =
      concatenateVectors(TimeOfFlight1, TimeOfFlight3);
  std::vector<int32_t> ExpectedDetectorID =
      concatenateVectors(DetectorID1, DetectorID3);
  std::vector<int64_t> ExpectedReferenceTime =
      concatenateVectors(ReferenceTime1, ReferenceTime3);
  std::vector<int32_t> ExpectedReferenceTimeIndex;
  int32_t numberOfEventsInFirstMessage =
      static_cast<int32_t>(DetectorID1.size());
  for (int32_t value : ReferenceTimeIndex1) {
    ExpectedReferenceTimeIndex.push_back(value);
  }
  for (int32_t value : ReferenceTimeIndex3) {
    ExpectedReferenceTimeIndex.push_back(value + numberOfEventsInFirstMessage);
  }

  // Test data in file matches what we originally put in the message
  EXPECT_THAT(EventTimeOffset, testing::ContainerEq(ExpectedTimeOfFlight))
      << "Expected event_time_offset dataset to contain the time of flight "
         "values from both messages";
  EXPECT_THAT(EventTimeZero, testing::ContainerEq(ExpectedReferenceTime))
      << "Expected event_time_zero dataset to contain the pulse time "
         "values from the message";
  EXPECT_THAT(EventIndex, testing::ContainerEq(ExpectedReferenceTimeIndex))
      << "Expected event_index dataset to contain the index of first event for "
         "the corresponding pulse ";
  EXPECT_EQ(EventIndex[ReferenceTime1.size()], numberOfEventsInFirstMessage)
      << "Expected the firts event_index for the second message to start at an "
         "index "
         "equal to the number of events in the first message ";
  EXPECT_THAT(EventID, testing::ContainerEq(ExpectedDetectorID))
      << "Expected event_id dataset to contain the detector ID "
         "values from both messages";
}

TEST_F(Event44WriterTests, PulseTimeIsRepeatedWithinSameMessage) {
  std::vector<int32_t> const TimeOfFlight = {101, 102, 201, 301, 401, 402, 403};
  std::vector<int32_t> const DetectorID = {101, 102, 201, 301, 401, 402, 403};
  std::vector<int64_t> const ReferenceTime = {1000, 1000, 2000, 2000};
  std::vector<int32_t> const ReferenceTimeIndex = {0, 2, 3, 4};
  // Repeated pulse times are currently not consolidated into a single value
  std::vector<int64_t> ExpectedReferenceTime = {1000, 1000, 2000, 2000};
  std::vector<int32_t> ExpectedReferenceTimeIndex = {0, 2, 3, 4};

  auto MessageBuffer =
      generateFlatbufferData("TestSource", 0, TimeOfFlight, DetectorID,
                             ReferenceTime, ReferenceTimeIndex);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev44::ev44_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage, false));
  } // These braces are required due to "h5.cpp"

  // Read data from the file
  auto EventTimeOffsetDataset = TestGroup.get_dataset("event_time_offset");
  auto EventTimeZeroDataset = TestGroup.get_dataset("event_time_zero");
  auto EventIndexDataset = TestGroup.get_dataset("event_index");
  auto EventIDDataset = TestGroup.get_dataset("event_id");
  std::vector<int32_t> EventTimeOffset(
      EventTimeOffsetDataset.dataspace().size());
  std::vector<int64_t> EventTimeZero(EventTimeZeroDataset.dataspace().size());
  std::vector<int32_t> EventIndex(EventIndexDataset.dataspace().size());
  std::vector<int32_t> EventID(EventIDDataset.dataspace().size());
  EventTimeOffsetDataset.read(EventTimeOffset);
  EventTimeZeroDataset.read(EventTimeZero);
  EventIndexDataset.read(EventIndex);
  EventIDDataset.read(EventID);

  // Test data in file matches what we originally put in the message
  EXPECT_THAT(EventTimeOffset, testing::ContainerEq(TimeOfFlight))
      << "Expected event_time_offset dataset to contain the time of flight "
         "values from the message";
  EXPECT_THAT(EventTimeZero, testing::ContainerEq(ExpectedReferenceTime))
      << "Expected event_time_zero dataset to contain the pulse time "
         "values from the message";
  EXPECT_THAT(EventIndex, testing::ContainerEq(ExpectedReferenceTimeIndex))
      << "Expected event_index dataset to contain the index of first event for "
         "the corresponding pulse ";
  EXPECT_EQ(EventIndex[0], 0)
      << "Expected first event_index value to be zero "
         "as the first pulse is always about the first message";
  EXPECT_THAT(EventID, testing::ContainerEq(DetectorID))
      << "Expected event_id dataset to contain the detector ID "
         "values from the message";
}

TEST_F(Event44WriterTests, LastPulseTimeIsRepeatedInSubsequentMessage) {
  std::vector<int32_t> TimeOfFlight1 = {101, 102, 201};
  std::vector<int32_t> TimeOfFlight2 = {301, 302, 303, 401, 501, 502};
  std::vector<int32_t> DetectorID1 = {101, 102, 201};
  std::vector<int32_t> DetectorID2 = {301, 302, 303, 401, 501, 502};
  std::vector<int64_t> ReferenceTime1 = {1000, 2000};
  std::vector<int64_t> ReferenceTime2 = {2000, 3000, 4000};
  std::vector<int32_t> ReferenceTimeIndex1 = {0, 2};
  std::vector<int32_t> ReferenceTimeIndex2 = {0, 3, 4};
  // Repeated pulse times are currently not consolidated into a single value
  std::vector<int64_t> ExpectedReferenceTime = {1000, 2000, 2000, 3000, 4000};
  std::vector<int32_t> ExpectedReferenceTimeIndex = {0, 2, 3, 6, 7};

  auto MessageBuffer1 =
      generateFlatbufferData("TestSource", 1, TimeOfFlight1, DetectorID1,
                             ReferenceTime1, ReferenceTimeIndex1);
  FileWriter::FlatbufferMessage TestMessage1(MessageBuffer1.data(),
                                             MessageBuffer1.size());
  auto MessageBuffer2 =
      generateFlatbufferData("TestSource", 2, TimeOfFlight2, DetectorID2,
                             ReferenceTime2, ReferenceTimeIndex2);
  FileWriter::FlatbufferMessage TestMessage2(MessageBuffer2.data(),
                                             MessageBuffer2.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev44::ev44_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage1, false));
    EXPECT_NO_THROW(Writer.write(TestMessage2, false));
  } // These braces are required due to "h5.cpp"

  // Read data from the file
  auto EventTimeOffsetDataset = TestGroup.get_dataset("event_time_offset");
  auto EventTimeZeroDataset = TestGroup.get_dataset("event_time_zero");
  auto EventIndexDataset = TestGroup.get_dataset("event_index");
  auto EventIDDataset = TestGroup.get_dataset("event_id");
  std::vector<int32_t> EventTimeOffset(
      EventTimeOffsetDataset.dataspace().size());
  std::vector<int64_t> EventTimeZero(EventTimeZeroDataset.dataspace().size());
  std::vector<int32_t> EventIndex(EventIndexDataset.dataspace().size());
  std::vector<int32_t> EventID(EventIDDataset.dataspace().size());
  EventTimeOffsetDataset.read(EventTimeOffset);
  EventTimeZeroDataset.read(EventTimeZero);
  EventIndexDataset.read(EventIndex);
  EventIDDataset.read(EventID);

  // Duplicate events
  std::vector<int32_t> ExpectedTimeOfFlight =
      concatenateVectors(TimeOfFlight1, TimeOfFlight2);
  std::vector<int32_t> ExpectedDetectorID =
      concatenateVectors(DetectorID1, DetectorID2);

  // Test data in file matches what we originally put in the message
  EXPECT_THAT(EventTimeOffset, testing::ContainerEq(ExpectedTimeOfFlight))
      << "Expected event_time_offset dataset to contain the time of flight "
         "values from both messages";
  EXPECT_THAT(EventTimeZero, testing::ContainerEq(ExpectedReferenceTime))
      << "Expected event_time_zero dataset to contain the pulse time "
         "values from the message";
  EXPECT_THAT(EventIndex, testing::ContainerEq(ExpectedReferenceTimeIndex))
      << "Expected event_index dataset to contain the index of first event for "
         "the corresponding pulse ";
  EXPECT_EQ(EventIndex[ReferenceTime1.size()],
            static_cast<int32_t>(DetectorID1.size()))
      << "Expected the firts event_index for the second message to start at an "
         "index "
         "equal to the number of events in the first message ";
  EXPECT_THAT(EventID, testing::ContainerEq(ExpectedDetectorID))
      << "Expected event_id dataset to contain the detector ID "
         "values from both messages";
}

TEST_F(Event44WriterTests, CuesFromTwoMessagesAreRecorded) {
  std::vector<int32_t> TimeOfFlight1 = {101, 102, 201};
  std::vector<int32_t> TimeOfFlight2 = {301, 302, 303, 401, 501, 502};
  std::vector<int32_t> DetectorID1 = {101, 102, 201};
  std::vector<int32_t> DetectorID2 = {301, 302, 303, 401, 501, 502};
  std::vector<int64_t> ReferenceTime1 = {1000, 2000};
  std::vector<int64_t> ReferenceTime2 = {3000, 4000, 5000};
  std::vector<int32_t> ReferenceTimeIndex1 = {0, 2};
  std::vector<int32_t> ReferenceTimeIndex2 = {0, 3, 4};
  auto MessageBuffer1 =
      generateFlatbufferData("TestSource", 1, TimeOfFlight1, DetectorID1,
                             ReferenceTime1, ReferenceTimeIndex1);
  FileWriter::FlatbufferMessage TestMessage1(MessageBuffer1.data(),
                                             MessageBuffer1.size());
  auto MessageBuffer2 =
      generateFlatbufferData("TestSource", 2, TimeOfFlight2, DetectorID2,
                             ReferenceTime2, ReferenceTimeIndex2);
  FileWriter::FlatbufferMessage TestMessage2(MessageBuffer2.data(),
                                             MessageBuffer2.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev44::ev44_Writer Writer;
    Writer.setCueInterval(1);
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage1, false));
    EXPECT_NO_THROW(Writer.write(TestMessage2, false));
  } // These braces are required due to "h5.cpp"

  // Read data from the file
  auto CueTimestampZeroDataset = TestGroup.get_dataset("cue_timestamp_zero");
  auto CueIndexDataset = TestGroup.get_dataset("cue_index");
  std::vector<int32_t> CueTimestampZero(
      CueTimestampZeroDataset.dataspace().size());
  std::vector<int32_t> CueIndex(CueIndexDataset.dataspace().size());
  CueTimestampZeroDataset.read(CueTimestampZero);
  CueIndexDataset.read(CueIndex);

  std::vector<int32_t> ExpectedCueTimestampZero = {2201, 5502};
  EXPECT_THAT(CueTimestampZero, testing::ContainerEq(ExpectedCueTimestampZero))
      << "Expected cue_timestamp_zero dataset to contain the timestamps "
         "calculated from pulse time plus offset";
  std::vector<int32_t> ExpectedCueIndex = {2, 8};
  EXPECT_THAT(CueIndex, testing::ContainerEq(ExpectedCueIndex))
      << "Expected cue_index dataset to contain the indices of the last event "
         "of every message";
}

TEST_F(Event44WriterTests, buffered_data_not_written) {
  std::vector<int32_t> TimeOfFlight1 = {101, 102, 201};
  std::vector<int32_t> DetectorID1 = {101, 102, 201};
  std::vector<int64_t> ReferenceTime1 = {1000, 2000};
  std::vector<int32_t> ReferenceTimeIndex1 = {0, 2};
  auto MessageBuffer1 =
      generateFlatbufferData("TestSource", 1, TimeOfFlight1, DetectorID1,
                             ReferenceTime1, ReferenceTimeIndex1);
  FileWriter::FlatbufferMessage TestMessage1(MessageBuffer1.data(),
                                             MessageBuffer1.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev44::ev44_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage1, true));
  } // These braces are required due to "h5.cpp"

  // Read data from the file
  auto EventTimeOffsetDataset = TestGroup.get_dataset("event_time_offset");
  auto EventTimeZeroDataset = TestGroup.get_dataset("event_time_zero");

  EXPECT_EQ(0, EventTimeOffsetDataset.dataspace().size());
  EXPECT_EQ(0, EventTimeZeroDataset.dataspace().size());
}
