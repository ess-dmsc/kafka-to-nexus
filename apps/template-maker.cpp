#include "FileWriterTask.h"
#include "JobCreator.h"
#include "MetaData/Tracker.h"
#include "Metrics/Metric.h"
#include "logger.h"
#include <ep01_epics_connection_generated.h>
#include <f144_logdata_generated.h>
#include <iostream>
#include <memory>
#include <utility>
#include <fstream>
#include <sstream>
#include <string>

using std::chrono_literals::operator""ms;


std::string readJsonFromFile(const std::string& filePath) {
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

int main([[maybe_unused]] int argc, [[maybe_unused]] char **argv) {
  using std::chrono_literals::operator""ms;
  std::cout << "Starting writing\n";

  std::unique_ptr<Metrics::IRegistrar> registrar =
      std::make_unique<FakeRegistrar>();
  auto tracker = std::make_shared<FakeTracker>();

  Command::StartInfo start_info;

  std::string jsonFilePath = "/home/jonas/code/nexus-json-templates/bifrost/bifrost-test.json";
  std::string example_json = readJsonFromFile(jsonFilePath);

  std::cout << "Loaded file\n";

  start_info.NexusStructure = example_json;
  start_info.JobID = "some_job_id";

  FileWriter::StreamerOptions streamer_options;
  streamer_options.StartTimestamp = time_point{0ms};
  streamer_options.StopTimestamp = time_point{1250ms};
  std::filesystem::path filepath{"../../template_test.hdf"};

  FileWriter::createFileWriterTemplate(start_info, filepath, registrar.get(), tracker);
  // stream_controller->start();

  // while (!stream_controller->isDoneWriting()) {
  //   std::cout << "Stream controller is writing\n";
  //   std::this_thread::sleep_for(std::chrono::milliseconds(500));
  // }

  std::cout << "Stream controller has finished writing\n";

  return 0;
}