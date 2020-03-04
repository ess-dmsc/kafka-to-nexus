// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \brief Test partition filtering.
///

#include "Stream/SourceFilter.h"
#include <chrono>
#include <thread>
#include <gtest/gtest.h>
#include <trompeloeil.hpp>
#include "FlatbufferReader.h"
#include "helpers/SetExtractorModule.h"

class MessageWriterStandIn : public Stream::MessageWriter {
public:
    MessageWriterStandIn() : MessageWriter(Metrics::Registrar("", {})) {}
    MAKE_MOCK1(addMessage, void(Stream::Message), override);
};

class SourceFilterTest : public ::testing::Test {
public:
    Stream::time_point StartTime{std::chrono::system_clock::now()};
    MessageWriterStandIn Writer;
    Stream::SourceFilter UnderTest{StartTime, std::chrono::system_clock::time_point::max(),
                                   &Writer};
};

TEST_F(SourceFilterTest, InitState) {
    EXPECT_FALSE(UnderTest.hasFinished());
}
using trompeloeil::_;

static class xxxFbReader : public FileWriter::FlatbufferReader {
    bool verify(FileWriter::FlatbufferMessage const &) const override {
        return true;
    }

    std::string
    source_name(FileWriter::FlatbufferMessage const &) const override {
        return "some_name";
    }

    uint64_t timestamp(FileWriter::FlatbufferMessage const &) const override {
        return xxxFbReader::Timestamp;
    }
    static void setTimestamp(uint64_t NewTime) {
        xxxFbReader::Timestamp = NewTime;
    }
private:
    static uint64_t Timestamp;
};

uint64_t xxxFbReader::Timestamp{1};

FileWriter::FlatbufferMessage generateMsg() {
    std::array<char, 9> SomeData{'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x'};
    setExtractorModule<xxxFbReader>("xxxx");
    return FileWriter::FlatbufferMessage(SomeData.data(), SomeData.size());
}

TEST_F(SourceFilterTest, InvalidMessage) {
    FORBID_CALL(Writer, addMessage(_));
    FileWriter::FlatbufferMessage TestMsg;
    UnderTest.filterMessage(std::move(TestMsg));
}

TEST_F(SourceFilterTest, MessageUnknownDest) {
    FORBID_CALL(Writer, addMessage(_));
    auto TestMsg = generateMsg();
    UnderTest.filterMessage(std::move(TestMsg));
}