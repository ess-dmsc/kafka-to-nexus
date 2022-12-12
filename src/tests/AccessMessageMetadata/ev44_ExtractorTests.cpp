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
#include "helpers/SetExtractorModule.h"

namespace EV44ExtractorTests {
using FileWriter::FlatbufferReaderRegistry::ReaderPtr;

static flatbuffers::DetachedBuffer
generateFlatbufferData(std::string const &SourceName = "TestSource",
                       int64_t const MessageID = 0,
                       std::vector<int32_t> const &TimeOfFlight = {0, 1, 2},
                       std::vector<int32_t> const &DetectorID = {0, 1, 2}) {
  flatbuffers::FlatBufferBuilder builder;

  auto FBSourceNameOffset = builder.CreateString(SourceName);
  auto FBTimeOfFlightOffset = builder.CreateVector(TimeOfFlight);
  auto FBDetectorIDOffset = builder.CreateVector(DetectorID);

  Event44MessageBuilder MessageBuilder(builder);
  MessageBuilder.add_source_name(FBSourceNameOffset);
  MessageBuilder.add_message_id(MessageID);
  MessageBuilder.add_time_of_flight(FBTimeOfFlightOffset);
  MessageBuilder.add_pixel_id(FBDetectorIDOffset);

  builder.Finish(MessageBuilder.Finish(), Event44MessageIdentifier());

  // Note, Release gives us a "DetachedBuffer" which owns the data
  return builder.Release();
}

/// Modifies the input vector such that it is the concatenation of itself with
/// itself
template <typename T> void repeatVector(std::vector<T> &InputVector) {
  InputVector.insert(InputVector.end(), InputVector.begin(), InputVector.end());
}

class Event44ReaderTests : public ::testing::Test {
public:
  void SetUp() override {
    ReaderUnderTest = std::make_unique<AccessMessageMetadata::ev44_Extractor>();
    setExtractorModule<AccessMessageMetadata::ev44_Extractor>("ev44");
  };

  std::unique_ptr<AccessMessageMetadata::ev44_Extractor> ReaderUnderTest;
};

TEST_F(Event44ReaderTests, ReaderReturnsSourceNameFromMessage) {
  std::string const TestSourceName = "TestSource";
  auto MessageBuffer = generateFlatbufferData(TestSourceName);
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());
  EXPECT_EQ(ReaderUnderTest->source_name(TestMessage), TestSourceName);
}

TEST_F(Event44ReaderTests, ReaderVerifiesValidMessage) {
  auto MessageBuffer = generateFlatbufferData();
  FileWriter::FlatbufferMessage TestMessage(MessageBuffer.data(),
                                            MessageBuffer.size());
  EXPECT_TRUE(ReaderUnderTest->verify(TestMessage));
}
} // namespace EV44ExtractorTests
