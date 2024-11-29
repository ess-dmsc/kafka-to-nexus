#include "FileWriterTask.h"
#include "JobCreator.h"
#include "MetaData/Tracker.h"
#include "Metrics/Metric.h"
#include "logger.h"
#include <CLI/CLI.hpp>
#include <ep01_epics_connection_generated.h>
#include <f144_logdata_generated.h>
#include <iostream>
#include <memory>
#include <utility>

using std::chrono_literals::operator""ms;

std::string readJsonFromFile(const std::string &filePath) {
  std::ifstream file(filePath);
  if (!file.is_open()) {
    throw std::runtime_error("Unable to open file: " + filePath);
  }
  std::stringstream buffer;
  buffer << file.rdbuf();
  file.close();
  return buffer.str();
}

class FakeRegistrar : public Metrics::IRegistrar {
public:
  void registerMetric([[maybe_unused]] Metrics::Metric &NewMetric,
                      [[maybe_unused]] std::vector<Metrics::LogTo> const
                          &SinkTypes) const override {}

  [[nodiscard]] std::unique_ptr<Metrics::IRegistrar> getNewRegistrar(
      [[maybe_unused]] std::string const &MetricsPrefix) const override {
    return std::make_unique<FakeRegistrar>();
  }
};

int main([[maybe_unused]] int argc, [[maybe_unused]] char **argv) {
  CLI::App app{"template-maker app"};
  std::string json_file;
  std::string instrument_name;
  app.add_option("-f, --file", json_file, "The JSON file to load");
  app.add_option("-i, --instrument", instrument_name, "The instrument name");
  CLI11_PARSE(app, argc, argv);

  std::cout << "Starting writing\n";

  std::unique_ptr<Metrics::IRegistrar> registrar =
      std::make_unique<FakeRegistrar>();
  auto tracker = std::make_shared<MetaData::Tracker>();

  Command::StartMessage start_info;

  std::cout << "Loaded file\n";

  if (!json_file.empty()) {
    start_info.NexusStructure = readJsonFromFile(json_file);
  } else {
    throw std::runtime_error("No JSON file provided");
  }
  if (!instrument_name.empty()) {
    start_info.InstrumentName = instrument_name;
  } else {
    throw std::runtime_error("No instrument name provided");
  }
  start_info.JobID = "some_job_id";

  std::filesystem::path filepath =
      fmt::format("../../nexus/{0}/{0}.hdf", instrument_name);

  FileWriter::createFileWriterTemplate(start_info, filepath, registrar.get(),
                                       tracker);

  std::cout << "Finished writing template\n";

  return 0;
}
