#include "FlatbufferMessage.h"
#include "FlatbufferReader.h"
#include <gtest/gtest.h>
#include <map>

namespace FileWriter {

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

static std::map<std::string, std::unique_ptr<FileWriter::FlatbufferReader>>
    FlatbufferReaders;

class MessageClassTest : public ::testing::Test {
public:
  void SetUp() override {
    FlatbufferReaders["temp"] =
        std::unique_ptr<FileWriter::FlatbufferReader>(new MsgDummyReader1);
    FlatbufferReaders["tmp2"] =
        std::unique_ptr<FileWriter::FlatbufferReader>(new InvalidReader);
  }
};

TEST_F(MessageClassTest, Success) {
  std::string TestKey("temp");
  std::unique_ptr<char[]> TestData(new char[8]);
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  auto CurrentMessage = FlatbufferMessage(TestData.get(), 8, FlatbufferReaders);
  EXPECT_TRUE(CurrentMessage.isValid());
  EXPECT_EQ(CurrentMessage.getTimestamp(), std::uint64_t(42));
  EXPECT_EQ(CurrentMessage.getSourceName(), "SomeSourceName");
  EXPECT_EQ(CurrentMessage.size(), size_t(8));
}

TEST_F(MessageClassTest, WrongFlatbufferID) {
  std::string TestKey("temp");
  std::string AltTestKey("temo");
  std::unique_ptr<char[]> TestData(new char[8]);
  std::memcpy(TestData.get() + 4, AltTestKey.c_str(), 4);
  ASSERT_THROW(FlatbufferMessage(TestData.get(), 8, FlatbufferReaders),
               UnknownFlatbufferID);
}

TEST_F(MessageClassTest, SizeTooSmall) {
  std::string TestKey("temp");
  std::unique_ptr<char[]> TestData(new char[8]);
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  ASSERT_THROW(FlatbufferMessage(TestData.get(), 7, FlatbufferReaders),
               FileWriter::BufferTooSmallError);
}

TEST_F(MessageClassTest, InvalidFlatbuffer) {
  std::vector<char> TestData{1, 0, 0, 0, 't', 'm', 'p', '2', 30, 30, 30, 30};
  ASSERT_THROW(FlatbufferMessage(TestData.data(), 12, FlatbufferReaders),
               FileWriter::NotValidFlatbuffer);
}
} // namespace FileWriter
