// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandSystem/CommandListener.h"
#include <gtest/gtest.h>

TEST(CommandListener, TimeoutOnPoll) {
  Command::CommandListener Listener(uri::URI("localhost:1111/no_topic_here"),
                                    {});
  // Poll # one
  auto PollResult = Listener.pollForCommand();
  EXPECT_EQ(PollResult.first, Kafka::PollStatus::TimedOut);
  EXPECT_EQ(PollResult.second.size(), 0u);
  // Poll # two
  PollResult = Listener.pollForCommand();
  EXPECT_EQ(PollResult.first, Kafka::PollStatus::TimedOut);
  EXPECT_EQ(PollResult.second.size(), 0u);
}
