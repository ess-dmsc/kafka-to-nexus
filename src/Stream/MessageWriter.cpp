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

#include "MessageWriter.h"

#include "SetThreadName.h"
#include "WriterModuleBase.h"
#include <utility>

namespace Stream {

using ModuleHash = MessageWriter::ModuleHash;

ModuleHash generateSrcHash(std::string const &Source,
                           std::string const &FlatbufferId) {
  return std::hash<std::string>{}(Source + FlatbufferId);
}

static const ModuleHash UnknownModuleHash{
    generateSrcHash("Unknown source", "Unknown fb-id")};

MessageWriter::MessageWriter(std::function<void()> FlushFunction,
                             duration FlushIntervalTime,
                             std::unique_ptr<Metrics::IRegistrar> registrar)
    : FlushDataFunction(std::move(FlushFunction)),
      _registrar(std::move(registrar)),
      FlushInterval(FlushIntervalTime),
      WriterThread(&MessageWriter::threadFunction, this) {

  WritesDone = std::make_shared<Metrics::Metric>(
      "writes_done", "Number of completed writes to HDF file.");
  _registrar->registerMetric(WritesDone, {Metrics::LogTo::CARBON});

  WriteErrors = std::make_shared<Metrics::Metric>(
      "write_errors", "Number of failed HDF file writes.", Metrics::Severity::ERROR);
  _registrar->registerMetric(WriteErrors, {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});

  ApproxQueuedWrites = std::make_shared<Metrics::Metric>(
      "approx_queued_writes", "Approximate number of writes queued up.");
  _registrar->registerMetric(ApproxQueuedWrites, {Metrics::LogTo::CARBON});

  ModuleErrorCounters[UnknownModuleHash] = std::make_shared<Metrics::Metric>(
      "error_unknown", "Unknown flatbuffer message.", Metrics::Severity::ERROR);
  _registrar->registerMetric(ModuleErrorCounters[UnknownModuleHash], {Metrics::LogTo::LOG_MSG});
}

MessageWriter::~MessageWriter() {
  RunThread.store(false);
  if (WriterThread.joinable()) {
    WriterThread.join();
  }
}

void MessageWriter::addMessage(Message const &Msg, bool is_buffered_message) {
  WriteJobs.enqueue(
      [=]() { writeMsgImpl(Msg.DestPtr, Msg.FbMsg, is_buffered_message); });
}

void MessageWriter::stop() {
  RunThread.store(false);
  if (WriterThread.joinable()) {
    WriterThread.join();
  }
}

void MessageWriter::writeMsgImpl(WriterModule::Base *ModulePtr,
                                 FileWriter::FlatbufferMessage const &Msg,
                                 bool is_buffered_message) {
  try {
    ModulePtr->write(Msg, is_buffered_message);
    WritesDone++;
  } catch (WriterModule::WriterException &E) {
    WriteErrors++;
    auto UsedHash = UnknownModuleHash;
    if (Msg.isValid()) {
      UsedHash = generateSrcHash(Msg.getSourceName(), Msg.getFlatbufferID());
      if (ModuleErrorCounters.find(UsedHash) == ModuleErrorCounters.end()) {
        auto Description = fmt::format(
            R"(Error writing fb.-msg with source name "{}" and flatbuffer id: {})",
            Msg.getSourceName(), Msg.getFlatbufferID());
        auto Name =
            "error_" + Msg.getSourceName() + "_" + Msg.getFlatbufferID();
        ModuleErrorCounters[UsedHash] = std::make_shared<Metrics::Metric>(
            Name, Description, Metrics::Severity::ERROR);
        _registrar->registerMetric(ModuleErrorCounters[UsedHash],
                                   {Metrics::LogTo::LOG_MSG});
      }
    }
    (*ModuleErrorCounters[UsedHash])++;
  } catch (std::exception &E) {
    WriteErrors++;
    Logger::Error("Unknown file writing error: {}", E.what());
  }
}

void MessageWriter::threadFunction() {
  setThreadName("writer");
  int CheckTimeCounter{0};
  JobType CurrentJob;
  time_point NextFlushTime{system_clock::now() + FlushInterval};
  auto FlushOperation = [&]() {
    auto Now = system_clock::now();
    if (Now >= NextFlushTime) {
      ApproxQueuedWrites = WriteJobs.size_approx();
      flushData();
      auto FlushPeriods = int((Now - NextFlushTime) / FlushInterval) + 1;
      NextFlushTime += FlushPeriods * FlushInterval;
    }
  };
  auto WriteOperation = [&]() {
    CheckTimeCounter = 0;
    while (WriteJobs.try_dequeue(CurrentJob)) {
      CurrentJob();
      ++CheckTimeCounter;
      if (CheckTimeCounter > MaxTimeCheckCounter) {
        FlushOperation();
        CheckTimeCounter = 0;
      }
    }
  };
  while (RunThread.load()) {
    WriteOperation();
    FlushOperation();
    if (!RunThread.load()) {
      break;
    }
    std::this_thread::sleep_for(SleepTime);
  }
  WriteOperation();
}

} // namespace Stream
