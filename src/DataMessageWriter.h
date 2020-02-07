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

#include "Metrics/Metric.h"
#include "Metrics/Registrar.h"
#include "ThreadedExecutor.h"
#include "WriteMessage.h"
#include "WriterModuleBase.h"
#include "logger.h"
#include <map>
#include <thread>

class DataMessageWriter {
public:
  DataMessageWriter(Metrics::Registrar const &MetricReg);
  void addMessage(WriteMessage Msg);
  using ModuleHash = size_t;

protected:
  void writeMsgImpl(intptr_t ModulePtr,
                    FileWriter::FlatbufferMessage const &Msg);
  SharedLogger Log{getLogger()};
  Metrics::Metric WritesDone{"writes_done",
                             "Number of completed writes to HDF file."};
  Metrics::Metric WriteErrors{"write_errors",
                              "Number of failed HDF file writes.",
                              Metrics::Severity::ERROR};
  std::map<ModuleHash, std::unique_ptr<Metrics::Metric>> ModuleErrorCounters;
  Metrics::Registrar Registrar;
  ThreadedExecutor Executor; // Must be last
};
