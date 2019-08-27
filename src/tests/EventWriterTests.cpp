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

#include <ev42_events_generated.h>
#include <gtest/gtest.h>

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
