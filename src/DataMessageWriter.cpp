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

#include "DataMessageWriter.h"
#include "WriterModuleBase.h"

using ModuleHash = DataMessageWriter::ModuleHash;

ModuleHash generateSrcHash(std::string Source, std::string FlatbufferId) {
  return std::hash<std::string>{}(Source + FlatbufferId);
}

static const ModuleHash UnknownModuleHash{
    generateSrcHash("Unknown source", "Unknown fb.-id")};

DataMessageWriter::DataMessageWriter(Metrics::Registrar const &MetricReg)
    : Registrar(MetricReg.getNewRegistrar("writer")) {
  Registrar.registerMetric(WritesDone, {Metrics::LogTo::CARBON});
  Registrar.registerMetric(WriteErrors,
                           {Metrics::LogTo::CARBON, Metrics::LogTo::LOG_MSG});
  ModuleErrorCounters[UnknownModuleHash] = std::make_unique<Metrics::Metric>(
      "error_unknown", "Unknown flatbuffer message.", Metrics::Severity::ERROR);
  Registrar.registerMetric(*ModuleErrorCounters[UnknownModuleHash],
                           {Metrics::LogTo::LOG_MSG});
}

void DataMessageWriter::addMessage(WriteMessage Msg) {
  Executor.SendWork([=]() { writeMsgImpl(Msg.DestId, Msg.FbMsg); });
}

void DataMessageWriter::writeMsgImpl(intptr_t ModulePtr,
                                     FileWriter::FlatbufferMessage const &Msg) {
  try {
    reinterpret_cast<WriterModule::Base *>(ModulePtr)->write(Msg);
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