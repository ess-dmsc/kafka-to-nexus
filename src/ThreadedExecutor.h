/* Copyright (C) 2018 European Spallation Source, ERIC. See LICENSE file */
//===----------------------------------------------------------------------===//
///
/// \file
///
/// \brief Simple threaded executor to simplify the use of threading in this lib
/// library.
///
//===----------------------------------------------------------------------===//

#pragma once

#include <concurrentqueue/concurrentqueue.h>
#include <functional>
#include <future>
#include <memory>
#include <thread>

class ThreadedExecutor {
private:
public:
  using JobType = std::function<void()>;
  explicit ThreadedExecutor(bool LowPriorityThreadExit = false)
      : LowPriorityExit(LowPriorityThreadExit), WorkerThread(ThreadFunction) {}
  ~ThreadedExecutor() {
    if (LowPriorityExit) {
      SendLowPrioWork([=]() { RunThread = false; });
    } else {
      SendWork([=]() { RunThread = false; });
    }
    WorkerThread.join();
  }
  void SendWork(JobType Task) { TaskQueue.enqueue(std::move(Task)); }
  size_t size_approx() { return TaskQueue.size_approx(); }
  void SendLowPrioWork(JobType Task) {
    LowPriorityTaskQueue.enqueue(std::move(Task));
  }

private:
  bool RunThread{true};
  std::function<void()> ThreadFunction{[=]() {
    while (RunThread) {
      JobType CurrentTask;
      if (TaskQueue.try_dequeue(CurrentTask)) {
        CurrentTask();
      } else if (LowPriorityTaskQueue.try_dequeue(CurrentTask)) {
        CurrentTask();
      } else {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(5ms);
      }
    }
  }};
  moodycamel::ConcurrentQueue<JobType> TaskQueue;
  moodycamel::ConcurrentQueue<JobType> LowPriorityTaskQueue;
  bool const LowPriorityExit{false};
  std::thread WorkerThread;
};
