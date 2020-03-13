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
  using WorkMessage = std::function<void()>;
  ThreadedExecutor(bool LowPrioThreadExit = false)
      : LowPrioExit(LowPrioThreadExit), WorkerThread(ThreadFunction) {}
  ~ThreadedExecutor() {
    if (LowPrioExit) {
      SendLowPrioWork([=]() { RunThread = false; });
    } else {
      SendWork([=]() { RunThread = false; });
    }
    WorkerThread.join();
  }
  void SendWork(WorkMessage Message) { MessageQueue.enqueue(Message); }
  size_t size_approx() { return MessageQueue.size_approx(); }
  void SendLowPrioWork(WorkMessage Message) {
    LowPrioMessageQueue.enqueue(Message);
  }

private:
  bool RunThread{true};
  std::function<void()> ThreadFunction{[=]() {
    // cppcheck-suppress internalAstError
    while (RunThread) {
      WorkMessage CurrentMessage;
      if (MessageQueue.try_dequeue(CurrentMessage)) {
        CurrentMessage();
      } else if (LowPrioMessageQueue.try_dequeue(CurrentMessage)) {
        CurrentMessage();
      } else {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(5ms);
      }
    }
  }};
  moodycamel::ConcurrentQueue<WorkMessage> MessageQueue;
  moodycamel::ConcurrentQueue<WorkMessage> LowPrioMessageQueue;
  bool const LowPrioExit{false};
  std::thread WorkerThread;
};
