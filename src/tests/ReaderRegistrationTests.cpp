// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <FlatbufferReader.h>
#include <gtest/gtest.h>

using namespace FileWriter;

using ReaderPtr = FlatbufferReaderRegistry::ReaderPtr;

class ReaderRegistrationTest : public ::testing::Test {
public:
  void SetUp() override {
    std::map<std::string, ReaderPtr> &Readers =
        FlatbufferReaderRegistry::getReaders();
    Readers.clear();
  }
};

class DummyReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const & /*Message*/) const override {
    return true;
  }
  std::string
  source_name(FlatbufferMessage const & /*Message*/) const override {
    return std::string();
  }
  std::uint64_t
  timestamp(FlatbufferMessage const & /*Message*/) const override {
    return 0;
  }
};

TEST_F(ReaderRegistrationTest, SimpleRegistration) {
  std::map<std::string, ReaderPtr> &Readers =
      FlatbufferReaderRegistry::getReaders();
  std::string TestKey("temp");
  EXPECT_EQ(Readers.size(), 0u);
  { FlatbufferReaderRegistry::Registrar<DummyReader> RegisterIt(TestKey); }
  EXPECT_EQ(Readers.size(), 1u);
  EXPECT_NE(Readers.find(TestKey), Readers.end());
}

TEST_F(ReaderRegistrationTest, SameKeyRegistration) {
  std::string TestKey("temp");
  { FlatbufferReaderRegistry::Registrar<DummyReader> RegisterIt(TestKey); }
  EXPECT_THROW(
      FlatbufferReaderRegistry::Registrar<DummyReader> RegisterIt(TestKey),
      std::runtime_error);
}

TEST_F(ReaderRegistrationTest, KeyTooShort) {
  std::string TestKey("tem");
  EXPECT_THROW(
      FlatbufferReaderRegistry::Registrar<DummyReader> RegisterIt(TestKey),
      std::runtime_error);
}

TEST_F(ReaderRegistrationTest, KeyTooLong) {
  std::string TestKey("tempp");
  EXPECT_THROW(
      FlatbufferReaderRegistry::Registrar<DummyReader> RegisterIt(TestKey),
      std::runtime_error);
}

TEST_F(ReaderRegistrationTest, StrKeyFound) {
  std::string TestKey("t3mp");
  { FlatbufferReaderRegistry::Registrar<DummyReader> RegisterIt(TestKey); }
  EXPECT_NE(FlatbufferReaderRegistry::find(TestKey).get(), nullptr);
}

TEST_F(ReaderRegistrationTest, StrKeyNotFound) {
  std::string TestKey("t3mp");
  { FlatbufferReaderRegistry::Registrar<DummyReader> RegisterIt(TestKey); }
  std::string FailKey("trump");
  EXPECT_THROW(FlatbufferReaderRegistry::find(FailKey), std::nested_exception);
}
