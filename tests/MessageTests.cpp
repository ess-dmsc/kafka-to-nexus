// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatbufferMessage.h"
#include "FlatbufferReader.h"
#include <gtest/gtest.h>
#include <map>

using namespace FileWriter;

using FlatbufferReaderRegistry::ReaderPtr;

class MessageClassTest : public ::testing::Test {
public:
  void SetUp() override {
    std::map<std::string, ReaderPtr> &Readers =
        FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    TestData = std::make_unique<uint8_t[]>(8);
  }
  const std::string TestKey{"temp"};
  std::unique_ptr<uint8_t[]> TestData{nullptr};
};

class MsgDummyReader1 : public FlatbufferReader {
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

class InvalidReader : public FlatbufferReader {
public:
  bool verify(FlatbufferMessage const & /*Message*/) const override {
    return false;
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

TEST_F(MessageClassTest, Success) {
  {
    FlatbufferReaderRegistry::Registrar<MsgDummyReader1> RegisterIt(TestKey);
  }
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  auto CurrentMessage = FlatbufferMessage(TestData.get(), 8);
  EXPECT_TRUE(CurrentMessage.isValid());
  EXPECT_EQ(CurrentMessage.getTimestamp(), std::int64_t(42));
  EXPECT_EQ(CurrentMessage.getSourceName(), "SomeSourceName");
  EXPECT_EQ(CurrentMessage.size(), size_t(8));
}

TEST_F(MessageClassTest, WrongFlatbufferID) {
  std::string AltTestKey("temo");
  {
    FlatbufferReaderRegistry::Registrar<MsgDummyReader1> RegisterIt(AltTestKey);
  }
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  ASSERT_THROW(FlatbufferMessage(TestData.get(), 8), UnknownFlatbufferID);
}

TEST_F(MessageClassTest, SizeTooSmall) {
  {
    FlatbufferReaderRegistry::Registrar<MsgDummyReader1> RegisterIt(TestKey);
  }
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  ASSERT_THROW(FlatbufferMessage(TestData.get(), 7),
               FileWriter::BufferTooSmallError);
}

TEST_F(MessageClassTest, InvalidFlatbuffer) {
  {
    FlatbufferReaderRegistry::Registrar<InvalidReader> RegisterIt(TestKey);
  }
  std::memcpy(TestData.get() + 4, TestKey.c_str(), 4);
  ASSERT_THROW(FlatbufferMessage(TestData.get(), 8),
               FileWriter::FlatbufferError);
}
