#include <FlatbufferMessage.h>
#include <ProcessMessageResult.h>
#include <Source.h>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <schemas/ev42/ev42_rw.h>
#include <trompeloeil.hpp>
#include "tests/helpers/StubWriterModule.h"

namespace ev42 {
#include "schemas/ev42_events_generated.h"
} // namespace ev42

using FileWriter::Source;
using FileWriter::HDFWriterModule;
using FileWriter::FlatbufferMessage;
using FileWriter::FlatbufferReaderRegistry::ReaderPtr;

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
  Source TestSource(SourceName, ModuleName, std::move(WriterModule));
  TestSource.setTopic(TopicName);
  ASSERT_EQ(TestSource.topic(), TopicName);
  ASSERT_EQ(TestSource.sourcename(), SourceName);
}

TEST_F(SourceTests, MovedSourceHasCorrectState) {
  std::string SourceName("TestSourceName");
  std::string TopicName("TestTopicName");
  std::string ModuleName("test");
  auto WriterModule = std::make_unique<StubWriterModule>();
  Source TestSource(SourceName, ModuleName, std::move(WriterModule));
  TestSource.setTopic(TopicName);
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
  Source TestSource(SourceName, ModuleName, std::move(WriterModule));
  TestSource.setTopic(TopicName);
  flatbuffers::FlatBufferBuilder Builder;
  ev42::EventMessageBuilder EventMessage(Builder);
  ev42::FinishEventMessageBuffer(Builder, EventMessage.Finish());
  FlatbufferMessage Message(
      reinterpret_cast<char *>(Builder.GetBufferPointer()), Builder.GetSize());
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
  Source TestSource(SourceName, ModuleName, std::move(WriterModule));
  TestSource.setTopic(TopicName);
  flatbuffers::FlatBufferBuilder Builder;
  ev42::EventMessageBuilder EventMessage(Builder);
  ev42::FinishEventMessageBuffer(Builder, EventMessage.Finish());
  FlatbufferMessage Message(
      reinterpret_cast<char *>(Builder.GetBufferPointer()), Builder.GetSize());
  ASSERT_EQ(FileWriter::ProcessMessageResult::ERR,
            TestSource.process_message(Message));
}

TEST_F(SourceTests, ProcessMessageWithNonMatchingSchemaIdReturnsError) {
  std::string SourceName("TestSourceName");
  std::string TopicName("TestTopicName");
  std::string ModuleName("test");
  auto WriterModule = std::make_unique<StubWriterModule>();
  Source TestSource(SourceName, ModuleName, std::move(WriterModule));
  TestSource.setTopic(TopicName);
  flatbuffers::FlatBufferBuilder Builder;
  ev42::EventMessageBuilder EventMessage(Builder);
  ev42::FinishEventMessageBuffer(Builder, EventMessage.Finish());
  FlatbufferMessage Message(
      reinterpret_cast<char *>(Builder.GetBufferPointer()), Builder.GetSize());
  ASSERT_EQ(FileWriter::ProcessMessageResult::ERR,
            TestSource.process_message(Message));
}

TEST_F(SourceTests, ProcessMessageWithoutWriterModuleReturnsError) {
  std::string SourceName("TestSourceName");
  std::string ModuleName("ev42");
  Source TestSource(SourceName, ModuleName, {nullptr});
  flatbuffers::FlatBufferBuilder Builder;
  ev42::EventMessageBuilder EventMessage(Builder);
  ev42::FinishEventMessageBuffer(Builder, EventMessage.Finish());
  FlatbufferMessage Message(
      reinterpret_cast<char *>(Builder.GetBufferPointer()), Builder.GetSize());
  ASSERT_EQ(FileWriter::ProcessMessageResult::ERR,
            TestSource.process_message(Message));
}
