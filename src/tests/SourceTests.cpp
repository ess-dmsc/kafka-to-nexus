// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "helpers/SetExtractorModule.h"
#include "helpers/StubWriterModule.h"
#include <FlatbufferMessage.h>
#include <ProcessMessageResult.h>
#include <Source.h>
#include <fb_metadata_extractors/ev42/ev42_Extractor.h>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

namespace FlatbufferMetadata {
#include <ev42_events_generated.h>
} // namespace FlatbufferMetadata

using FileWriter::FlatbufferMessage;
using FileWriter::Source;
using FileWriter::FlatbufferReaderRegistry::ReaderPtr;
using WriterModule::Base;

flatbuffers::DetachedBuffer createEventMessageBuffer() {
  flatbuffers::FlatBufferBuilder Builder;
  FlatbufferMetadata::EventMessageBuilder EventMessage(Builder);
  EventMessage.add_pulse_time(
      1); // avoid 0 pulse time which is detected as a validation error
  Builder.Finish(EventMessage.Finish(),
                 FlatbufferMetadata::EventMessageIdentifier());

  // Note, Release gives us a "DetachedBuffer" which owns the data
  return Builder.Release();
}

class SourceTests : public ::testing::Test {
public:
  void SetUp() override {
    setExtractorModule<FlatbufferMetadata::ev42_Extractor>("ev42");
  };
};

class WriterModuleMock : public StubWriterModule {
public:
  MAKE_MOCK1(write, void(FlatbufferMessage const &), override);
};

TEST_F(SourceTests, ConstructorSetsMembers) {
  std::string SourceName("TestSourceName");
  std::string TopicName("TestTopicName");
  std::string ModuleName("test");
  auto WriterModule = std::make_unique<StubWriterModule>();
  Source TestSource(SourceName, ModuleName, TopicName, std::move(WriterModule));
  ASSERT_EQ(TestSource.topic(), TopicName);
  ASSERT_EQ(TestSource.sourcename(), SourceName);
}

TEST_F(SourceTests, MovedSourceHasCorrectState) {
  std::string SourceName("TestSourceName");
  std::string TopicName("TestTopicName");
  std::string ModuleName("test");
  auto WriterModule = std::make_unique<StubWriterModule>();
  Source TestSource(SourceName, ModuleName, TopicName, std::move(WriterModule));
  auto TestSource2 = std::move(TestSource);
  ASSERT_EQ(TestSource2.topic(), TopicName);
  ASSERT_EQ(TestSource2.sourcename(), SourceName);
}

TEST_F(SourceTests, ProcessMessagePassesMessageToWriterModule) {
  std::string SourceName("TestSourceName");
  std::string TopicName("TestTopicName");
  std::string ModuleName("ev42");
  auto WriterModule = std::make_unique<WriterModuleMock>();
  REQUIRE_CALL(*WriterModule, write(ANY(FlatbufferMessage const &)));
  Source TestSource(SourceName, ModuleName, TopicName, std::move(WriterModule));
  auto MessageBuffer = createEventMessageBuffer();
  FileWriter::FlatbufferMessage Message(
      reinterpret_cast<const char *>(MessageBuffer.data()),
      MessageBuffer.size());
  ASSERT_EQ(FileWriter::ProcessMessageResult::OK,
            TestSource.process_message(Message));
}

TEST_F(SourceTests, ProcessMessageReturnsErrorIfWriterModuleReturnsError) {
  std::string SourceName("TestSourceName");
  std::string TopicName("TestTopicName");
  std::string ModuleName("ev42");
  auto WriterModule = std::make_unique<WriterModuleMock>();
  REQUIRE_CALL(*WriterModule, write(ANY(FlatbufferMessage const &)))
      .THROW(WriterModule::WriterException("IO Error"));
  Source TestSource(SourceName, ModuleName, TopicName, std::move(WriterModule));
  auto MessageBuffer = createEventMessageBuffer();
  FileWriter::FlatbufferMessage Message(
      reinterpret_cast<const char *>(MessageBuffer.data()),
      MessageBuffer.size());
  ASSERT_EQ(FileWriter::ProcessMessageResult::ERR,
            TestSource.process_message(Message));
}

TEST_F(SourceTests, ProcessMessageWithNonMatchingSchemaIdReturnsError) {
  std::string SourceName("TestSourceName");
  std::string TopicName("TestTopicName");
  std::string ModuleName("test");
  auto WriterModule = std::make_unique<StubWriterModule>();
  Source TestSource(SourceName, ModuleName, TopicName, std::move(WriterModule));
  auto MessageBuffer = createEventMessageBuffer();
  FileWriter::FlatbufferMessage Message(
      reinterpret_cast<const char *>(MessageBuffer.data()),
      MessageBuffer.size());
  ASSERT_EQ(FileWriter::ProcessMessageResult::ERR,
            TestSource.process_message(Message));
}
