#include "FlatbufferMessage.h"
#include "FlatbufferReader.h"
#include <gtest/gtest.h>
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
  std::uint64_t timestamp(FlatbufferMessage const &Message) const override {
    return 42;
  }
};

class InvalidReader : public FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override { return false; }
  std::string source_name(FlatbufferMessage const &Message) const override {
    return "SomeSourceName";
  }
  std::uint64_t timestamp(FlatbufferMessage const &Message) const override {
    return 42;
  }
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
  EXPECT_EQ(CurrentMessage.size(), size_t(8));
}

TEST_F(MessageClassTest, WrongFlatbufferID) {
  std::string TestKey("temp");
  std::string AltTestKey("temo");
  {
    FlatbufferReaderRegistry::Registrar<MsgDummyReader1> RegisterIt(AltTestKey);
  }
  std::unique_ptr<char[]> TestData(new char[8]);
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  ASSERT_THROW(FlatbufferMessage(TestData.get(), 8), UnknownFlatbufferID);
}

TEST_F(MessageClassTest, SizeTooSmall) {
  std::string TestKey("temp");
  { FlatbufferReaderRegistry::Registrar<MsgDummyReader1> RegisterIt(TestKey); }
  std::unique_ptr<char[]> TestData(new char[8]);
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  ASSERT_THROW(FlatbufferMessage(TestData.get(), 7),
               FileWriter::BufferTooSmallError);
}

TEST_F(MessageClassTest, InvalidFlatbuffer) {
  std::string TestKey("temp");
  { FlatbufferReaderRegistry::Registrar<InvalidReader> RegisterIt(TestKey); }
  std::unique_ptr<char[]> TestData(new char[8]);
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  ASSERT_THROW(FlatbufferMessage(TestData.get(), 8),
               FileWriter::NotValidFlatbuffer);
}
} // namespace FileWriter
