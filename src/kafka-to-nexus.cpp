#include "kafka-to-nexus.h"
#include "CLIOptions.h"
#include "FlatbufferReader.h"
#include "HDFWriterModule.h"
#include "MainOpt.h"
#include "Master.h"
#include "logger.h"
#include "uri.h"
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

  fmt::print("kafka-to-nexus {:.7} (ESS, BrightnESS)\n"
             "  Contact: dominik.werder@psi.ch, michele.brambilla@psi.ch\n\n",
             GIT_COMMIT);
  CLI::App App{
      "Writes NeXus files in a format specified with a json template.\n"
      "Writer modules can be used to populate the file from Kafka topics.\n"};
  auto Options = std::unique_ptr<MainOpt>(new MainOpt());
  Options->init();
  setCLIOptions(App, *Options);

  CLI11_PARSE(App, argc, argv);
  Options->parse_config_file();

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

  setup_logger_from_options(*Options);

  FileWriter::Master Master(*Options);
  std::thread MasterThread([&Master] {
    try {
      Master.run();
    } catch (std::system_error const &e) {
      LOG(Sev::Critical,
          "std::system_error  code: {}  category: {}  message: {}  what: {}",
          e.code().value(), e.code().category().name(), e.code().message(),
          e.what());
      throw;
    } catch (std::runtime_error const &e) {
      LOG(Sev::Critical, "std::runtime_error  what: {}", e.what());
      throw;
    } catch (std::exception const &e) {
      LOG(Sev::Critical, "std::exception  what: {}", e.what());
      throw;
    }
  });

  while (not Master.RunLoopExited()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (GotSignal) {
      LOG(Sev::Notice, "SIGNAL {}", SignalId);
      Master.stop();
      GotSignal = false;
      break;
    }
  }

  MasterThread.join();
  return 0;
}
