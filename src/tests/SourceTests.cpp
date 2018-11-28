#include <FlatbufferMessage.h>
#include <ProcessMessageResult.h>
#include <Source.h>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <schemas/ev42/ev42_rw.h>
#include <trompeloeil.hpp>

namespace ev42 {
#include "schemas/ev42_events_generated.h"
}

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

class WriterModuleDummy : public HDFWriterModule {
public:
  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override {}
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override {
    return InitResult::OK();
  }
  InitResult reopen(hdf5::node::Group &HDFGrup) override {
    return InitResult::OK();
  }
  WriteResult write(FlatbufferMessage const &Message) override {
    return WriteResult::OK();
  }
  std::int32_t flush() override { return 0; }
  std::int32_t close() override { return 0; }
  void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                 int mpi_rank) override {}
};

class WriterModuleMock : public WriterModuleDummy {
public:
  MAKE_MOCK1(write, WriteResult(FlatbufferMessage const &), override);
};

TEST_F(SourceTests, ConstructorSetsMembers) {
  std::string SourceName("TestSourceName");
  std::string TopicName("TestTopicName");
  std::string ModuleName("test");
  auto WriterModule = std::make_unique<WriterModuleDummy>();
  Source TestSource(SourceName, ModuleName, std::move(WriterModule));
  TestSource.setTopic(TopicName);
  ASSERT_EQ(TestSource.topic(), TopicName);
  ASSERT_EQ(TestSource.sourcename(), SourceName);
}

TEST_F(SourceTests, MovedSourceHasCorrectState) {
  std::string SourceName("TestSourceName");
  std::string TopicName("TestTopicName");
  std::string ModuleName("test");
  auto WriterModule = std::make_unique<WriterModuleDummy>();
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
  REQUIRE_CALL(*WriterModule, write(ANY(FlatbufferMessage const &)))
      .RETURN(FileWriter::HDFWriterModule::WriteResult::OK());
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
      .RETURN(FileWriter::HDFWriterModule::WriteResult::ERROR_IO());
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
  auto WriterModule = std::make_unique<WriterModuleDummy>();
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
