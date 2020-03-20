// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <concurrentqueue/concurrentqueue.h>
#include <functional>
#include <future>
#include <memory>
#include <thread>

/// \brief Class for executing jobs in a seperate "worker thread".
///
/// This implementation uses two work/task queues: high priority and low
/// priority. High priority jobs will be executed first before any low priority
/// tasks are attempted.

/// \note The execution order of jobs in a queue can not be guaranteed. In
/// fact, it is likely that all the tasks produced by one thread will be
/// completed before the tasks produced by another thread.
class ThreadedExecutor {
private:
public:
  using JobType = std::function<void()>;
  /// \brief Constructor of ThreadedExecutor.
  ///
  /// \param LowPriorityThreadExit If set to true, will put the exit thread
  /// task (created by the destructor) in the low priority queue.
  explicit ThreadedExecutor(bool LowPriorityThreadExit = false)
      : LowPriorityExit(LowPriorityThreadExit), WorkerThread(ThreadFunction) {}

  /// \brief Destructor, see constructor for details on exiting the thread
  /// when calling the destructor.
  ~ThreadedExecutor() {
    if (LowPriorityExit) {
      SendLowPriorityWork([=]() { RunThread = false; });
    } else {
      SendWork([=]() { RunThread = false; });
    }
    WorkerThread.join();
  }
  /// \brief Put tasks in the high priority queue.
  ///
  /// \param Task The std::function that will be executed when processing the
  /// task.
  void SendWork(JobType Task) { TaskQueue.enqueue(std::move(Task)); }
  /// \brief Put tasks in the low priority queue.
  ///
  /// \param Task The std::function that will be executed when processing the
  /// task.
  void SendLowPriorityWork(JobType Task) {
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
