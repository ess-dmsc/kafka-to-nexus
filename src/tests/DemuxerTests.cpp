// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "helpers/StubWriterModule.h"
#include <DemuxTopic.h>
#include <gtest/gtest.h>

using namespace FileWriter;

using FileWriter::FlatbufferReaderRegistry::ReaderPtr;

class MessageTimeExtractionTest : public ::testing::Test {
public:
  void SetUp() override {
    std::map<std::string, ReaderPtr> &Readers =
        FlatbufferReaderRegistry::getReaders();
    Readers.clear();
  }
};

class DemuxerDummyReader2 : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const & /*Message*/) const override {
    return true;
  }
  std::string
  source_name(FlatbufferMessage const & /*Message*/) const override {
    return "SomeSourceName";
  }
  std::uint64_t
  timestamp(FlatbufferMessage const & /*Message*/) const override {
    return 42;
  }
};

TEST_F(MessageTimeExtractionTest, Success) {
  std::string TestKey("temp");
  {
    FlatbufferReaderRegistry::Registrar<DemuxerDummyReader2> RegisterIt(
        TestKey);
  }
  auto TestData = std::make_unique<char[]>(8);
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  FileWriter::FlatbufferMessage CurrentMessage(TestData.get(), 8);
  EXPECT_TRUE(CurrentMessage.isValid());
  EXPECT_EQ(CurrentMessage.getSourceName(), "SomeSourceName");
  EXPECT_EQ(CurrentMessage.getTimestamp(), 42ul);
}

TEST_F(MessageTimeExtractionTest, WrongFlatbufferID) {
  std::string TestKey("temp");
  std::string MessageKey("temr");
  {
    FlatbufferReaderRegistry::Registrar<DemuxerDummyReader2> RegisterIt(
        TestKey);
  }
  auto TestData = std::make_unique<char[]>(8);
  std::memcpy(TestData.get() + 4, MessageKey.c_str(), 4);

  EXPECT_THROW(FileWriter::FlatbufferMessage(TestData.get(), 8),
               UnknownFlatbufferID);
}

TEST_F(MessageTimeExtractionTest, WrongMsgSize) {
  std::string TestKey("temp");
  {
    FlatbufferReaderRegistry::Registrar<DemuxerDummyReader2> RegisterIt(
        TestKey);
  }
  auto TestData = std::make_unique<char[]>(8);
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  EXPECT_THROW(FileWriter::FlatbufferMessage(TestData.get(), 7),
               BufferTooSmallError);
}

class DemuxerTest : public ::testing::Test {
public:
  void SetUp() override {
    std::map<std::string, ReaderPtr> &Readers =
        FlatbufferReaderRegistry::getReaders();
    Readers.clear();
  }
};

TEST_F(DemuxerTest, Success) {
  std::string TestKey("temp");
  std::string SourceName("SomeSourceName");
  {
    FlatbufferReaderRegistry::Registrar<DemuxerDummyReader2> RegisterIt(
        TestKey);
  }
  auto TestData = std::make_unique<char[]>(8);
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  FileWriter::FlatbufferMessage CurrentMessage(TestData.get(), 8);
  ProcessMessageResult Result{ProcessMessageResult::ERR};
  DemuxTopic TestDemuxer("SomeTopicName");
  auto Writer = ::std::make_unique<StubWriterModule>();
  Source DummySource(SourceName, TestKey, std::move(Writer));
  auto UsedHash = DummySource.getHash();
  TestDemuxer.add_source(std::move(DummySource));
  ASSERT_EQ(TestDemuxer.sources().size(), size_t(1));
  EXPECT_FALSE(TestDemuxer.sources().find(UsedHash) ==
               TestDemuxer.sources().end());
  EXPECT_NO_THROW(Result = TestDemuxer.process_message(CurrentMessage));
  EXPECT_EQ(Result, ProcessMessageResult::OK);
  EXPECT_TRUE(TestDemuxer.messages_processed.load() == size_t(1));
  EXPECT_TRUE(TestDemuxer.error_message_too_small.load() == size_t(0));
  EXPECT_TRUE(TestDemuxer.error_no_flatbuffer_reader.load() == size_t(0));
  EXPECT_TRUE(TestDemuxer.error_no_source_instance.load() == size_t(0));
}

TEST_F(DemuxerTest, WrongFlatbufferID) {
  std::string TestKey("temp");
  std::string SourceName("SomeSourceName");
  {
    FlatbufferReaderRegistry::Registrar<DemuxerDummyReader2> RegisterIt(
        TestKey);
  }
  auto TestData = std::make_unique<char[]>(8);
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  FileWriter::FlatbufferMessage CurrentMessage(TestData.get(), 8);
  ProcessMessageResult Result{ProcessMessageResult::OK};
  DemuxTopic TestDemuxer("SomeTopicName");
  auto Writer = ::std::make_unique<StubWriterModule>();
  std::string AltKey("temi");
  Source DummySource(SourceName, AltKey, std::move(Writer));
  auto UsedHash = DummySource.getHash();
  TestDemuxer.add_source(std::move(DummySource));
  ASSERT_EQ(TestDemuxer.sources().size(), size_t(1));
  EXPECT_FALSE(TestDemuxer.sources().find(UsedHash) ==
               TestDemuxer.sources().end());
  EXPECT_NO_THROW(Result = TestDemuxer.process_message(CurrentMessage));
  EXPECT_EQ(Result, ProcessMessageResult::ERR);
  EXPECT_EQ(TestDemuxer.messages_processed.load(), size_t(0));
  EXPECT_EQ(TestDemuxer.error_message_too_small.load(), size_t(0));
  EXPECT_EQ(TestDemuxer.error_no_flatbuffer_reader.load(), size_t(0));
  EXPECT_EQ(TestDemuxer.error_no_source_instance.load(), size_t(1));
}

TEST_F(DemuxerTest, WrongSourceName) {
  std::string TestKey("temp");
  std::string SourceName("WrongSourceName");
  {
    FlatbufferReaderRegistry::Registrar<DemuxerDummyReader2> RegisterIt(
        TestKey);
  }
  auto TestData = std::make_unique<char[]>(8);
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  FileWriter::FlatbufferMessage CurrentMessage(TestData.get(), 8);
  ProcessMessageResult Result{ProcessMessageResult::OK};
  DemuxTopic TestDemuxer("SomeTopicName");
  auto Writer = ::std::make_unique<StubWriterModule>();
  Source DummySource(SourceName, TestKey, std::move(Writer));
  TestDemuxer.add_source(std::move(DummySource));
  EXPECT_NO_THROW(Result = TestDemuxer.process_message(CurrentMessage));
  EXPECT_EQ(Result, ProcessMessageResult::ERR);
  EXPECT_TRUE(TestDemuxer.messages_processed.load() == size_t(0));
  EXPECT_TRUE(TestDemuxer.error_message_too_small.load() == size_t(0));
  EXPECT_TRUE(TestDemuxer.error_no_flatbuffer_reader.load() == size_t(0));
  EXPECT_TRUE(TestDemuxer.error_no_source_instance.load() == size_t(1));
}

TEST_F(DemuxerTest, RemovingExistingSourceIsSuccessful) {
  std::string TestKey("temp");
  std::string SourceName("SourceName");
  {
    FlatbufferReaderRegistry::Registrar<DemuxerDummyReader2> RegisterIt(
        TestKey);
  }
  DemuxTopic TestDemuxer("SomeTopicName");
  auto Writer = ::std::make_unique<StubWriterModule>();
  Source DummySource(SourceName, TestKey, std::move(Writer));
  auto DummySourceHash = DummySource.getHash();

  TestDemuxer.add_source(std::move(DummySource));
  EXPECT_TRUE(TestDemuxer.removeSource(DummySourceHash));
}

TEST_F(DemuxerTest, RemovingNonExistingSourceIsUnSuccessful) {
  std::string TestKey("temp");
  std::string SourceName("SourceName");
  {
    FlatbufferReaderRegistry::Registrar<DemuxerDummyReader2> RegisterIt(
        TestKey);
  }
  DemuxTopic TestDemuxer("SomeTopicName");
  auto Writer = ::std::make_unique<StubWriterModule>();
  Source DummySource(SourceName, TestKey, std::move(Writer));
  auto DummySourceHash = DummySource.getHash();

  EXPECT_FALSE(TestDemuxer.removeSource(DummySourceHash))
      << "Source was never added so we expect false to be returned when we try "
         "to remove it";
}
