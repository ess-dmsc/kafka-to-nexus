// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \brief Do the actual file writing.
///

#pragma once

#include "Message.h"
#include "Metrics/Metric.h"
#include "Metrics/Registrar.h"
#include "TimeUtility.h"
#include "logger.h"
#include <map>
#include <moodycamel/concurrentqueue.h>
#include <thread>

namespace WriterModule {
class Base;
}

namespace Stream {

/// \brief Implements the writing of flatbuffer messages to disk.
///
/// We can only have one writer per (HDF5) file.
class MessageWriter {
public:
  explicit MessageWriter(std::function<void()> FlushFunction,
                         duration FlushIntervalTime,
                         std::unique_ptr<Metrics::IRegistrar> registrar);

  virtual ~MessageWriter();

  virtual void addMessage(Message const &Msg, bool is_buffered_message);

  /// \brief Tell the writer thread to stop.
  ///
  /// Blocking. The thread might take a while to stop.
  void stop();

  using ModuleHash = size_t;

  /// \brief Return the approximate number of writes queued.
  auto nrOfWritesQueued() const { return WriteJobs.size_approx(); };

  auto nrOfWritesDone() const { return int64_t(WritesDone); };
  auto nrOfWriteErrors() const { return int64_t(WriteErrors); };
  auto nrOfWriterModulesWithErrors() const {
    return ModuleErrorCounters.size();
  }

  void runJob(std::function<void()> Job) { WriteJobs.enqueue(Job); }

protected:
  virtual void writeMsgImpl(WriterModule::Base *ModulePtr,
                            FileWriter::FlatbufferMessage const &Msg,
                            bool is_buffered_message);
  virtual void threadFunction();

  virtual void flushData() { FlushDataFunction(); };
  std::function<void()> FlushDataFunction;

  Metrics::Metric WritesDone{"writes_done",
                             "Number of completed writes to HDF file."};
  Metrics::Metric WriteErrors{"write_errors",
                              "Number of failed HDF file writes.",
                              Metrics::Severity::ERROR};
  Metrics::Metric ApproxQueuedWrites{"approx_queued_writes",
                                     "Approximate number of writes queued up."};
  std::map<ModuleHash, std::unique_ptr<Metrics::Metric>> ModuleErrorCounters;
  std::unique_ptr<Metrics::IRegistrar> _registrar;

  using JobType = std::function<void()>;
  moodycamel::ConcurrentQueue<JobType> WriteJobs;
  std::atomic_bool RunThread{true};
  const duration SleepTime{10ms};
  duration FlushInterval{5s};
  const int MaxTimeCheckCounter{200};
  std::thread WriterThread; // Must be last
};

} // namespace Stream
