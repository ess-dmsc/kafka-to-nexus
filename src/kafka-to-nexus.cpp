// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CLIOptions.h"
#include "FlatbufferReader.h"
#include "HDFWriterModule.h"
#include "JobCreator.h"
#include "MainOpt.h"
#include "Master.h"
#include "URI.h"
#include "Version.h"
#include "logger.h"
#include <CLI/CLI.hpp>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>

// These should only be visible in this translation unit
static std::atomic_bool GotSignal{false};
static std::atomic_int SignalId{0};

void signal_handler(int Signal) {
  GotSignal = true;
  SignalId = Signal;
}

int main(int argc, char **argv) {
  CLI::App App{fmt::format(
      "kafka-to-nexus {:.7} (ESS, BrightnESS)\n"
      "https://github.com/ess-dmsc/kafka-to-nexus\n\n"
      "Writes NeXus files in a format specified with a json template.\n"
      "Writer modules can be used to populate the file from Kafka topics.\n",
      GetVersion())};
  auto Options = std::make_unique<MainOpt>();
  Options->init();
  setCLIOptions(App, *Options);

  try {
    App.parse(argc, argv);
  } catch (const CLI::ParseError &e) {
    // Do nothing, we only care about the version flag in this first pass.
  }

  if (Options->PrintVersion) {
    fmt::print("{}\n", GetVersion());
    return 0;
  }
  App.clear();

  CLI11_PARSE(App, argc, argv);
  setupLoggerFromOptions(*Options);
  auto Logger = getLogger();

  if (!Options->CommandsJsonFilename.empty()) {
    Options->parseJsonCommands();
  }

  if (Options->ListWriterModules) {
    fmt::print("Registered writer/reader classes\n");
    fmt::print("\n--Identifiers of FlatbufferReader instances\n");
    for (auto &ReaderPair :
         FileWriter::FlatbufferReaderRegistry::getReaders()) {
      fmt::print("---- {}\n", ReaderPair.first);
    }
    fmt::print("\n--Identifiers of HDFWriterModule factories\n");
    for (auto &WriterPair :
         FileWriter::HDFWriterModuleRegistry::getFactories()) {
      fmt::print("---- {}\n", WriterPair.first);
    }
    fmt::print("\nDone, exiting\n");
    return 0;
  }

  if (Options->use_signal_handler) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
  }
  FileWriter::Master Master(*Options,
                            std::make_unique<FileWriter::JobCreator>());
  std::thread MasterThread([&Master, Logger] {
    try {
      Master.run();
    } catch (std::system_error const &e) {
      Logger->critical(
          "std::system_error  code: {}  category: {}  message: {}  what: {}",
          e.code().value(), e.code().category().name(), e.code().message(),
          e.what());
      throw;
    } catch (std::runtime_error const &e) {
      Logger->critical("std::runtime_error  what: {}", e.what());
      throw;
    } catch (std::exception const &e) {
      Logger->critical("std::exception  what: {}", e.what());
      throw;
    }
  });

  while (!Master.runLoopExited()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (GotSignal) {
      Logger->debug("SIGNAL {}", SignalId);
      Master.stop();
      GotSignal = false;
      break;
    }
  }

  MasterThread.join();
  Logger->flush();
  return 0;
}
