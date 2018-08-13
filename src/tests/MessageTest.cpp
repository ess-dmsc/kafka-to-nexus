#include "FlatbufferMessage.h"
#include <gtest/gtest.h>
#include "FlatbufferReader.h"
#include <map>

namespace FileWriter {
  
  using FlatbufferReaderRegistry::ReaderPtr;
  
  class MessageClassTest : public ::testing::Test {
  public:
    void SetUp() override {
      std::map<std::string, ReaderPtr> &Readers =
      FlatbufferReaderRegistry::getReaders();
      Readers.clear();
    }
  };
  
  class MsgDummyReader1 : public FlatbufferReader {
  public:
    bool verify(FlatbufferMessage const &Message) const override { return true; }
    std::string source_name(FlatbufferMessage const &Message) const override {
      return "SomeSourceName";
    }
    std::uint64_t timestamp(FlatbufferMessage const &Message) const override { return 42; }
  };
  
  TEST_F(MessageClassTest, Success) {
    std::string TestKey("temp");
    { FlatbufferReaderRegistry::Registrar<MsgDummyReader1> RegisterIt(TestKey); }
    std::unique_ptr<char[]> TestData(new char[8]);
    std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
    auto CurrentMessage = FlatbufferMessage(TestData.get(), 8);
    EXPECT_TRUE(CurrentMessage.isValid());
    EXPECT_EQ(CurrentMessage.getTimestamp(), std::uint64_t(42));
    EXPECT_EQ(CurrentMessage.getSourceName(), "SomeSourceName");
    EXPECT_EQ(CurrentMessage.size(), 8);
  }
  
  TEST_F(MessageClassTest, WrongKey) {
    std::string TestKey("temp");
    std::string AltTestKey("temo");
    { FlatbufferReaderRegistry::Registrar<MsgDummyReader1> RegisterIt(AltTestKey); }
    std::unique_ptr<char[]> TestData(new char[8]);
    std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
    ASSERT_THROW(FlatbufferMessage(TestData.get(), 8), UnknownFlatbufferID);
  }
//
//  TEST_F(MessageTimeExtractionTest, WrongKey) {
//    std::string TestKey("temp");
//    std::string MessageKey("temr");
//    { FlatbufferReaderRegistry::Registrar<DummyReader2> RegisterIt(TestKey); }
//    char *TestData = new char[8];
//    std::memcpy(TestData + 4, MessageKey.c_str(), 4);
//    FileWriter::Msg CurrentMessage = Msg::owned(TestData, 8);
//    MessageTimestamp MessageInfo;
//    EXPECT_THROW(getMessageTime(CurrentMessage), std::runtime_error);
//  }
//
//  TEST_F(MessageTimeExtractionTest, WrongMsgSize) {
//    std::string TestKey("temp");
//    { FlatbufferReaderRegistry::Registrar<DummyReader2> RegisterIt(TestKey); }
//    char *TestData = new char[8];
//    std::memcpy(TestData + 4, TestKey.c_str(), 4);
//    FileWriter::Msg CurrentMessage = Msg::owned(TestData, 7);
//    MessageTimestamp MessageInfo;
//    EXPECT_THROW(getMessageTime(CurrentMessage), std::runtime_error);
//  }
//
//  class DemuxerTest : public ::testing::Test {
//  public:
//    void SetUp() override {
//      std::map<std::string, ReaderPtr> &Readers =
//      FlatbufferReaderRegistry::getReaders();
//      Readers.clear();
//    }
//  };
//
//  TEST_F(DemuxerTest, Success) {
//    std::string TestKey("temp");
//    std::string SourceName("SomeSourceName");
//    { FlatbufferReaderRegistry::Registrar<DummyReader2> RegisterIt(TestKey); }
//    char *TestData = new char[8];
//    std::memcpy(TestData + 4, TestKey.c_str(), 4);
//    FileWriter::Msg CurrentMessage = Msg::owned(TestData, 8);
//    ProcessMessageResult Result;
//    DemuxTopic TestDemuxer("SomeTopicName");
//    DummyWriter::ptr Writer(new DummyWriter);
//    Source DummySource(SourceName, std::move(Writer));
//    TestDemuxer.add_source(std::move(DummySource));
//    ASSERT_EQ(TestDemuxer.sources().size(), size_t(1));
//    EXPECT_NO_THROW(TestDemuxer.sources().at(SourceName));
//    EXPECT_NO_THROW(Result =
//                    TestDemuxer.process_message(std::move(CurrentMessage)));
//    EXPECT_EQ(Result, ProcessMessageResult::OK);
//    EXPECT_TRUE(TestDemuxer.messages_processed.load() == size_t(1));
//    EXPECT_TRUE(TestDemuxer.error_message_too_small.load() == size_t(0));
//    EXPECT_TRUE(TestDemuxer.error_no_flatbuffer_reader.load() == size_t(0));
//    EXPECT_TRUE(TestDemuxer.error_no_source_instance.load() == size_t(0));
//  }
//
//  TEST_F(DemuxerTest, WrongSourceName) {
//    std::string TestKey("temp");
//    std::string SourceName("WrongSourceName");
//    { FlatbufferReaderRegistry::Registrar<DummyReader2> RegisterIt(TestKey); }
//    char *TestData = new char[8];
//    std::memcpy(TestData + 4, TestKey.c_str(), 4);
//    FileWriter::Msg CurrentMessage = Msg::owned(TestData, 8);
//    ProcessMessageResult Result;
//    DemuxTopic TestDemuxer("SomeTopicName");
//    DummyWriter::ptr Writer(new DummyWriter);
//    Source DummySource(SourceName, std::move(Writer));
//    TestDemuxer.add_source(std::move(DummySource));
//    EXPECT_NO_THROW(Result =
//                    TestDemuxer.process_message(std::move(CurrentMessage)));
//    EXPECT_EQ(Result, ProcessMessageResult::ERR);
//    EXPECT_TRUE(TestDemuxer.messages_processed.load() == size_t(0));
//    EXPECT_TRUE(TestDemuxer.error_message_too_small.load() == size_t(0));
//    EXPECT_TRUE(TestDemuxer.error_no_flatbuffer_reader.load() == size_t(0));
//    EXPECT_TRUE(TestDemuxer.error_no_source_instance.load() == size_t(1));
//  }
//
//  TEST_F(DemuxerTest, WrongKey) {
//    std::string TestKey("temp");
//    std::string AltKey("temo");
//    std::string SourceName("SomeSourceName");
//    { FlatbufferReaderRegistry::Registrar<DummyReader2> RegisterIt(AltKey); }
//    char *TestData = new char[8];
//    std::memcpy(TestData + 4, TestKey.c_str(), 4);
//    FileWriter::Msg CurrentMessage = Msg::owned(TestData, 8);
//    ProcessMessageResult Result;
//    DemuxTopic TestDemuxer("SomeTopicName");
//    DummyWriter::ptr Writer(new DummyWriter);
//    Source DummySource(SourceName, std::move(Writer));
//    TestDemuxer.add_source(std::move(DummySource));
//    EXPECT_NO_THROW(Result =
//                    TestDemuxer.process_message(std::move(CurrentMessage)));
//    EXPECT_EQ(Result, ProcessMessageResult::ERR);
//    EXPECT_TRUE(TestDemuxer.messages_processed.load() == size_t(0));
//    EXPECT_TRUE(TestDemuxer.error_message_too_small.load() == size_t(0));
//    EXPECT_TRUE(TestDemuxer.error_no_flatbuffer_reader.load() == size_t(1));
//    EXPECT_TRUE(TestDemuxer.error_no_source_instance.load() == size_t(0));
//  }
//
//  TEST_F(DemuxerTest, MessageTooSmall) {
//    std::string TestKey("temp");
//    std::string SourceName("SomeSourceName");
//    { FlatbufferReaderRegistry::Registrar<DummyReader2> RegisterIt(TestKey); }
//    char *TestData = new char[8];
//    std::memcpy(TestData + 4, TestKey.c_str(), 4);
//    FileWriter::Msg CurrentMessage = Msg::owned(TestData, 7);
//    ProcessMessageResult Result;
//    DemuxTopic TestDemuxer("SomeTopicName");
//    DummyWriter::ptr Writer(new DummyWriter);
//    Source DummySource(SourceName, std::move(Writer));
//    TestDemuxer.add_source(std::move(DummySource));
//    EXPECT_NO_THROW(Result =
//                    TestDemuxer.process_message(std::move(CurrentMessage)));
//    EXPECT_EQ(Result, ProcessMessageResult::ERR);
//    EXPECT_TRUE(TestDemuxer.messages_processed.load() == size_t(0));
//    EXPECT_TRUE(TestDemuxer.error_message_too_small.load() == size_t(1));
//    EXPECT_TRUE(TestDemuxer.error_no_flatbuffer_reader.load() == size_t(0));
//    EXPECT_TRUE(TestDemuxer.error_no_source_instance.load() == size_t(0));
//  }
  
} // namespace FileWriter
