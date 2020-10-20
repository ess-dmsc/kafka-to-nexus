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
#include "WriterModuleBase.h"

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
                             Metrics::Registrar const &MetricReg)
    : FlushDataFunction(FlushFunction),
      Registrar(MetricReg.getNewRegistrar("writer")),
      WriterThread(&MessageWriter::threadFunction, this),
      FlushInterval(FlushIntervalTime) {
  Registrar.registerMetric(WritesDone, {Metrics::LogTo::CARBON});
  Registrar.registerMetric(WriteErrors,
                           {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  ModuleErrorCounters[UnknownModuleHash] = std::make_unique<Metrics::Metric>(
      "error_unknown", "Unknown flatbuffer message.", Metrics::Severity::ERROR);
  Registrar.registerMetric(*ModuleErrorCounters[UnknownModuleHash],
                           {Metrics::LogTo::LOG_MSG});
}

MessageWriter::~MessageWriter() {
  RunThread = false;
  if (WriterThread.joinable()) {
    WriterThread.join();
  }
}

void MessageWriter::addMessage(Message const &Msg) {
  WriteJobs.enqueue([=]() { writeMsgImpl(Msg.DestPtr, Msg.FbMsg); });
}

void MessageWriter::stop() { RunThread = false; }

void MessageWriter::writeMsgImpl(WriterModule::Base *ModulePtr,
                                 FileWriter::FlatbufferMessage const &Msg) {
  try {
    ModulePtr->write(Msg);
    WritesDone++;
  } catch (WriterModule::WriterException &E) {
    WriteErrors++;
    auto UsedHash = UnknownModuleHash;
    if (Msg.isValid()) {
      UsedHash = generateSrcHash(Msg.getSourceName(), Msg.getFlatbufferID());
      if (ModuleErrorCounters.find(UsedHash) == ModuleErrorCounters.end()) {
        auto Description = "Error writing fb.-msg with source name \"" +
                           Msg.getSourceName() +
                           "\" and flatbuffer id: " + Msg.getFlatbufferID();
        auto Name =
            "error_" + Msg.getSourceName() + "_" + Msg.getFlatbufferID();
        ModuleErrorCounters[UsedHash] = std::make_unique<Metrics::Metric>(
            Name, Description, Metrics::Severity::ERROR);
        Registrar.registerMetric(*ModuleErrorCounters[UnknownModuleHash],
                                 {Metrics::LogTo::LOG_MSG});
      }
    }
    (*ModuleErrorCounters[UsedHash])++;
  } catch (std::exception &E) {
    WriteErrors++;
    Log->critical("Unknown file writing error: {}", E.what());
  }
}

void MessageWriter::threadFunction() {
  int CheckTimeCounter{0};
  JobType CurrentJob;
  time_point NextFlushTime{system_clock::now() + FlushInterval};
  auto FlushOperation = [&]() {
    auto Now = system_clock::now();
    if (Now >= NextFlushTime) {
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
  while (RunThread) {
    WriteOperation();
    FlushOperation();
    if (not RunThread) {
      break;
    }
    std::this_thread::sleep_for(SleepTime);
  }
  WriteOperation();
}

} // namespace Stream
