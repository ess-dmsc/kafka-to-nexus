// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "helpers/StubWriterModule.h"
#include <FlatbufferMessage.h>
#include <ProcessMessageResult.h>
#include <Source.h>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <schemas/ev42/ev42_rw.h>
#include <trompeloeil.hpp>

namespace ev42 {
#include "schemas/ev42_events_generated.h"
} // namespace ev42

using FileWriter::FlatbufferMessage;
using FileWriter::HDFWriterModule;
using FileWriter::Source;
using FileWriter::FlatbufferReaderRegistry::ReaderPtr;

flatbuffers::DetachedBuffer createEventMessageBuffer() {
  flatbuffers::FlatBufferBuilder Builder;
  ev42::EventMessageBuilder EventMessage(Builder);
  EventMessage.add_pulse_time(
      1); // avoid 0 pulse time which is detected as a validation error
  Builder.Finish(EventMessage.Finish(), ev42::EventMessageIdentifier());

  // Note, Release gives us a "DetachedBuffer" which owns the data
  return Builder.Release();
}

class SourceTests : public ::testing::Test {
public:
  void SetUp() override {
    auto &Readers = FileWriter::FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    FileWriter::FlatbufferReaderRegistry::Registrar<
        FileWriter::Schemas::ev42::FlatbufferReader>
        RegisterIt("ev42");
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
      .THROW(FileWriter::HDFWriterModuleRegistry::WriterException("IO Error"));
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
