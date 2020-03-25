// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ThreadedExecutor.h"
#include <gtest/gtest.h>

class ThreadedExecutorTest : public ::testing::Test {};

TEST_F(ThreadedExecutorTest, RunHighPriorityJob) {
  bool SomeVariable{false};
  {
    ThreadedExecutor Executor;
    Executor.sendWork([&SomeVariable]() { SomeVariable = true; });
  }
  EXPECT_TRUE(SomeVariable);
}

TEST_F(ThreadedExecutorTest, RunLowPriorityJob) {
  bool SomeVariable{false};
  {
    bool LowPriorityExit{true};
    ThreadedExecutor Executor{LowPriorityExit};
    Executor.sendLowPriorityWork([&SomeVariable]() { SomeVariable = true; });
  }
  EXPECT_TRUE(SomeVariable);
}

TEST_F(ThreadedExecutorTest, LowPriorityExit) {
  {
    bool LowPriorityExit{true};
    ThreadedExecutor Executor(LowPriorityExit);
  }
  SUCCEED();
}

TEST_F(ThreadedExecutorTest, HighPriorityExit) {
  {
    bool LowPriorityExit{false};
    ThreadedExecutor Executor(LowPriorityExit);
  }
  SUCCEED();
}
