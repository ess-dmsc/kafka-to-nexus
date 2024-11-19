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
#include <AccessMessageMetadata/ev44/ev44_Extractor.h>
#include <FlatbufferMessage.h>
#include <Source.h>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

namespace AccessMessageMetadata {
#include <ev44_events_generated.h>
} // namespace AccessMessageMetadata

using FileWriter::FlatbufferMessage;
using FileWriter::Source;
using FileWriter::FlatbufferReaderRegistry::ReaderPtr;
using WriterModule::Base;

class SourceTests : public ::testing::Test {
public:
  void SetUp() override {
    setExtractorModule<AccessMessageMetadata::ev44_Extractor>("ev44");
  };
};

class WriterModuleMock : public StubWriterModule {
public:
  MAKE_MOCK2(writeImpl, void(FlatbufferMessage const &, [[maybe_unused]] bool is_buffered_message), override);
};

TEST_F(SourceTests, ConstructorSetsMembers) {
  std::string SourceName("TestSourceName");
  std::string TopicName("TestTopicName");
  std::string FlatbufferId("fbid");
  std::string ModuleName("test");
  auto WriterModule = std::make_unique<StubWriterModule>();
  Source TestSource(SourceName, FlatbufferId, ModuleName, TopicName,
                    std::move(WriterModule));
  ASSERT_EQ(TestSource.topic(), TopicName);
  ASSERT_EQ(TestSource.sourcename(), SourceName);
}

TEST_F(SourceTests, MovedSourceHasCorrectState) {
  std::string SourceName("TestSourceName");
  std::string TopicName("TestTopicName");
  std::string FlatbufferId("fbid");
  std::string ModuleName("test");
  auto WriterModule = std::make_unique<StubWriterModule>();
  Source TestSource(SourceName, FlatbufferId, ModuleName, TopicName,
                    std::move(WriterModule));
  auto TestSource2 = std::move(TestSource);
  ASSERT_EQ(TestSource2.topic(), TopicName);
  ASSERT_EQ(TestSource2.sourcename(), SourceName);
}
