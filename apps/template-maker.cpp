#include "FileWriterTask.h"
#include "JobCreator.h"
#include "MetaData/Tracker.h"
#include "Metrics/Metric.h"
#include "logger.h"
#include <ep01_epics_connection_generated.h>
#include <f144_logdata_generated.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
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

class FakeTracker : public MetaData::ITracker {
public:
  void
  registerMetaData([[maybe_unused]] MetaData::ValueBase NewMetaData) override {}
  void clearMetaData() override {}
  void
  writeToJSONDict([[maybe_unused]] nlohmann::json &JSONNode) const override {}
  void
  writeToHDF5File([[maybe_unused]] hdf5::node::Group &RootNode) const override {
  }
};

int main(int argc, char **argv) {
  CLI::App app{"template-maker app"};
  std::string json_file;
  std::string InstrumentName;
  app.add_option("-f, --file", json_file, "The JSON file to load");
  app.add_option("-i, --instrument", InstrumentName, "The instrument name");
  CLI11_PARSE(app, argc, argv);

  using std::chrono_literals::operator""ms;
  std::cout << "Starting writing\n";

  std::unique_ptr<Metrics::IRegistrar> registrar =
      std::make_unique<FakeRegistrar>();
  auto tracker = std::make_shared<FakeTracker>();

  Command::StartInfo start_info;

  std::string example_json = readJsonFromFile(json_file);

  std::cout << "Loaded file\n";

  if (!json_file.empty()) {
    start_info.NexusStructure = readJsonFromFile(json_file);
  } else {
    std::throw_with_nested(std::runtime_error("No JSON file provided"));
  }
  }
  if (!InstrumentName.empty()) {
    start_info.InstrumentName = InstrumentName;
  } else {
    std::throw_with_nested(std::runtime_error("No instrument name provided"));
  }
  start_info.JobID = "some_job_id";

  FileWriter::StreamerOptions streamer_options;
  streamer_options.StartTimestamp = time_point{0ms};
  streamer_options.StopTimestamp = time_point{1250ms};
  std::filesystem::path filepath{"../../nexus_templates/" + InstrumentName +
                                 "/" + InstrumentName + ".hdf"};

  FileWriter::createFileWriterTemplate(start_info, filepath, registrar.get(),
                                       tracker);

  std::cout << "Finished writing template\n";

  return 0;
}
