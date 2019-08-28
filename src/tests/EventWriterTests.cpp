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

#include "helpers/HDFFileTestHelper.h"
#include "schemas/ev42/ev42_rw.h"

using namespace FileWriter::Schemas;
using FileWriter::FlatbufferReaderRegistry::ReaderPtr;

flatbuffers::DetachedBuffer
GenerateFlatbufferData(std::string const &SourceName = "TestSource",
                       uint64_t const MessageID = 0,
                       uint64_t const PulseTime = 0,
                       std::vector<uint32_t> const &TimeOfFlight = {0, 1, 2},
                       std::vector<uint32_t> const &DetectorID = {0, 1, 2}) {
  flatbuffers::FlatBufferBuilder builder;

  auto FBSourceNameOffset = builder.CreateString(SourceName);
  auto FBTimeOfFlightOffset = builder.CreateVector(TimeOfFlight);
  auto FBDetectorIDOffset = builder.CreateVector(DetectorID);

  EventMessageBuilder MessageBuilder(builder);
  MessageBuilder.add_source_name(FBSourceNameOffset);
  MessageBuilder.add_message_id(MessageID);
  MessageBuilder.add_pulse_time(PulseTime);
  MessageBuilder.add_time_of_flight(FBTimeOfFlightOffset);
  MessageBuilder.add_detector_id(FBDetectorIDOffset);

  builder.Finish(MessageBuilder.Finish(), EventMessageIdentifier());

  // Note, Release gives us a "DetachedBuffer" which owns the data
  return builder.Release();
}

class EventReaderTests : public ::testing::Test {
public:
  void SetUp() override {
    ReaderUnderTest = std::make_unique<ev42::FlatbufferReader>();
    std::map<std::string, ReaderPtr> &Readers =
        FileWriter::FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    FileWriter::FlatbufferReaderRegistry::Registrar<ev42::FlatbufferReader>
        RegisterIt("ev42");
  };

  std::unique_ptr<ev42::FlatbufferReader> ReaderUnderTest;
};

TEST_F(EventReaderTests, ReaderReturnsSourceNameFromMessage) {
  std::string const TestSourceName = "TestSource";
  auto MessageBuffer = GenerateFlatbufferData(TestSourceName);
  FileWriter::FlatbufferMessage TestMessage(
      reinterpret_cast<const char *>(MessageBuffer.data()),
      MessageBuffer.size());
  EXPECT_EQ(ReaderUnderTest->source_name(TestMessage), TestSourceName);
}

TEST_F(EventReaderTests, ReaderReturnsPulseTimeAsMessageTimestamp) {
  uint64_t PulseTime = 42;
  auto MessageBuffer = GenerateFlatbufferData("TestSource", 0, PulseTime);
  FileWriter::FlatbufferMessage TestMessage(
      reinterpret_cast<const char *>(MessageBuffer.data()),
      MessageBuffer.size());
  EXPECT_EQ(ReaderUnderTest->timestamp(TestMessage), PulseTime);
}

TEST_F(EventReaderTests, ReaderVerifiesValidMessage) {
  auto MessageBuffer = GenerateFlatbufferData();
  FileWriter::FlatbufferMessage TestMessage(
      reinterpret_cast<const char *>(MessageBuffer.data()),
      MessageBuffer.size());
  EXPECT_TRUE(ReaderUnderTest->verify(TestMessage));
}

class EventWriterTests : public ::testing::Test {
public:
  void SetUp() override {
    File = HDFFileTestHelper::createInMemoryTestFile("EventWriterTestFile.nxs");
    TestGroup = File.H5File.root().create_group(TestGroupName);
  };

  void TearDown() override { File.close(); };
  FileWriter::HDFFile File;
  hdf5::node::Group TestGroup;
  std::string const TestGroupName = "test_group";
};

using FileWriter::HDFWriterModule_detail::InitResult;

TEST_F(EventWriterTests, WriterInitialisesFileWithNXEventDataDatasets) {
  {
    ev42::HDFWriterModule Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
  }
  ASSERT_TRUE(File.H5File.root().has_group(TestGroupName));
  EXPECT_TRUE(TestGroup.has_dataset("event_time_offset"));
  EXPECT_TRUE(TestGroup.has_dataset("event_time_zero"));
  EXPECT_TRUE(TestGroup.has_dataset("event_index"));
  EXPECT_TRUE(TestGroup.has_dataset("event_id"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_index"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_timestamp_zero"));
}

TEST_F(EventWriterTests, WriterFailsToReopenGroupWhichWasNeverInitialised) {
  ev42::HDFWriterModule Writer;
  EXPECT_FALSE(Writer.reopen(TestGroup) == InitResult::OK);
}

TEST_F(EventWriterTests, WriterSuccessfullyReopensGroupWhichWasInitialised) {
  ev42::HDFWriterModule Writer;
  EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
}

TEST_F(EventWriterTests, WriterReportsFailureIfTryToInitialiseTwice) {
  ev42::HDFWriterModule Writer;
  EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
}

TEST_F(EventWriterTests, WriterSuccessfullyRecordsEventDataFromSingleMessage) {
  // Create a single event message with data we can later check is recorded in
  // the file
  uint64_t const PulseTime = 42;
  std::vector<uint32_t> const TimeOfFlight = {0, 1, 2};
  std::vector<uint32_t> const DetectorID = {3, 4, 5};
  auto MessageBuffer = GenerateFlatbufferData("TestSource", 0, PulseTime,
                                              TimeOfFlight, DetectorID);
  FileWriter::FlatbufferMessage TestMessage(
      reinterpret_cast<const char *>(MessageBuffer.data()),
      MessageBuffer.size());

  // Create writer and give it the message to write
  {
    ev42::HDFWriterModule Writer;
    EXPECT_TRUE(Writer.init_hdf(TestGroup, "{}") == InitResult::OK);
    EXPECT_TRUE(Writer.reopen(TestGroup) == InitResult::OK);
    EXPECT_NO_THROW(Writer.write(TestMessage));
  }  // These braces are required due to "h5.cpp"

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
