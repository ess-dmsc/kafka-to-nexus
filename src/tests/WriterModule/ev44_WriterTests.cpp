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
    std::vector<int32_t> const &TimeOfFlight = {0, 1, 2},
    std::vector<int32_t> const &DetectorID = {0, 1, 2},
    std::vector<int64_t> const &reference_time = {34151332, 65271216, 73474746},
    std::vector<int32_t> const &reference_time_index = {0, 1, 2}) {
  flatbuffers::FlatBufferBuilder builder;

  auto FBSourceNameOffset = builder.CreateString(SourceName);
  auto FBTimeOfFlightOffset = builder.CreateVector(TimeOfFlight);
  auto FBDetectorIDOffset = builder.CreateVector(DetectorID);
  auto FBReferenceTime = builder.CreateVector(reference_time);
  auto FBReferenceTimeIndex = builder.CreateVector(reference_time_index);

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

/// Modifies the input vector such that it is the concatenation of itself with
/// itself
template <typename T> void repeatVector(std::vector<T> &InputVector) {
  InputVector.insert(InputVector.end(), InputVector.begin(), InputVector.end());
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

TEST_F(Event44WriterTests, WriterInitialisesFileWithNXEventDataDatasets) {
  {
    WriterModule::ev44::ev44_Writer Writer;
    // Tell writer module to write ADC pulse debug data
    Writer.parse_config(R"({"adc_pulse_debug": true})");
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
  std::vector<int32_t> const TimeOfFlight = {0, 1, 2};
  std::vector<int32_t> const DetectorID = {3, 4, 5};
  auto MessageBuffer =
      generateFlatbufferData("TestSource", 0, TimeOfFlight, DetectorID);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev44::ev44_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage));
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
  EXPECT_EQ(EventTimeZero.size(), 3U)
      << "Expected event_time_zero to contain a "
         "single value, as we wrote a single "
         "message";
  EXPECT_EQ(EventIndex.size(), 1U)
      << "Expected event_index to contain a single "
         "value, as we wrote a single message";
  EXPECT_EQ(EventIndex[0], 0) << "Expected single event_index value to be zero "
                                 "as we wrote only one message";
  EXPECT_THAT(EventID, testing::ContainerEq(DetectorID))
      << "Expected event_id dataset to contain the detector ID "
         "values from the message";
}

TEST_F(Event44WriterTests, WriterSuccessfullyRecordsEventDataFromTwoMessages) {
  // Create a single event message with data we can later check is recorded in
  // the file
  std::vector<int32_t> TimeOfFlight = {0, 1, 2};
  std::vector<int32_t> DetectorID = {3, 4, 5};
  auto MessageBuffer =
      generateFlatbufferData("TestSource", 0, TimeOfFlight, DetectorID);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());

  // Create writer and give it the message to write
  {
    WriterModule::ev44::ev44_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup) == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage));
    EXPECT_NO_THROW(Writer.write(TestMessage));
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

  // Repeat the input value vectors as the same message should be written twice
  repeatVector(TimeOfFlight);
  repeatVector(DetectorID);

  // Test data in file matches what we originally put in the message
  EXPECT_THAT(EventTimeOffset, testing::ContainerEq(TimeOfFlight))
      << "Expected event_time_offset dataset to contain the time of flight "
         "values from both messages";
  EXPECT_EQ(EventTimeZero.size(), 6U)
      << "Expected event_time_zero to contain a "
         "two values, as we wrote two "
         "messages";
  EXPECT_EQ(EventIndex.size(), 2U) << "Expected event_index to contain two "
                                      "values, as we wrote two messages";
  EXPECT_EQ(EventIndex[1], 3) << "Expected second message to start at index 3 "
                                 "as there were 3 events in the first message";
  EXPECT_THAT(EventID, testing::ContainerEq(DetectorID))
      << "Expected event_id dataset to contain the detector ID "
         "values from both messages";
}

TEST_F(Event44WriterTests, WriteCues) {}