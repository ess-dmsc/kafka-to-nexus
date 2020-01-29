// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Master.h"
#include <gtest/gtest.h>
#include <memory>

using namespace FileWriter;

std::string StartCommand{R"""({
    "cmd": "filewriter_new",
    "broker": "localhost:9092",
    "job_id": "1234",
    "file_attributes": {"file_name": "output_file1.nxs"},
    "nexus_structure": { }
  })"""};

std::string StopCommand{R"""({
    "cmd": "filewriter_stop",
    "job_id": "1234"
  })"""};

TEST(ParseCommandTests, IfCommandStringIsParseableThenDoesNotThrow) {
  ASSERT_NO_THROW(parseCommand(StartCommand));
}

TEST(ParseCommandTests, IfCommandStringIsNotParseableThenThrows) {
  ASSERT_THROW(parseCommand("{Invalid: JSON"), std::runtime_error);
}

TEST(GetNewStateTests, IfIdleThenOnStartCommandStartIsRequested) {
  FileWriterState CurrentState = States::Idle();
  auto const NewState =
      getNextState(StartCommand, std::chrono::milliseconds{0}, CurrentState);

  ASSERT_TRUE(mpark::get_if<States::StartRequested>(&NewState));
}

TEST(GetNewStateTests, IfWritingThenOnStartCommandNoStateChange) {
  FileWriterState CurrentState = States::Writing();
  auto const NewState =
      getNextState(StartCommand, std::chrono::milliseconds{0}, CurrentState);

  ASSERT_TRUE(mpark::get_if<States::Writing>(&NewState));
}

TEST(GetNewStateTests, IfWritingThenOnStopCommandStopIsRequested) {
  FileWriterState CurrentState = States::Writing();
  auto const NewState =
      getNextState(StopCommand, std::chrono::milliseconds{0}, CurrentState);

  ASSERT_TRUE(mpark::get_if<States::StopRequested>(&NewState));
}

TEST(GetNewStateTests, IfIdleThenOnStopCommandNoStateChange) {
  FileWriterState CurrentState = States::Idle();
  auto const NewState =
      getNextState(StopCommand, std::chrono::milliseconds{0}, CurrentState);

  ASSERT_TRUE(mpark::get_if<States::Idle>(&NewState));
}
