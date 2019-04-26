#pragma once

#include "MasterInterface.h"
#include "StreamMaster.h"
#include "Streamer.h"

class MockMasterI : public FileWriter::MasterInterface {

public:
  void run() override {}
  void stop() override {}
  void handle_command_message(std::unique_ptr<FileWriter::Msg> msg) override {}
  void handle_command(std::string const &command) override {}
  void statistics() override{};
  std::string getFileWriterProcessId() const override { return ProcessId; }
  bool runLoopExited() override { return false; }
  MainOpt &getMainOpt() override { return MainOptInst; }
  std::shared_ptr<KafkaW::ProducerTopic> getStatusProducer() override {
    return nullptr;
  }

  void addStreamMaster(
      std::unique_ptr<FileWriter::StreamMaster<FileWriter::Streamer>>)
      override {}
  void stopStreamMasters() override {}
  std::unique_ptr<FileWriter::StreamMaster<FileWriter::Streamer>> &
  getStreamMasterForJobID(std::string const &JobID_) override {
    static std::unique_ptr<FileWriter::StreamMaster<FileWriter::Streamer>>
        NotFound;
    return NotFound;
  }

private:
  MainOpt MainOptInst;
  std::string ProcessId;
};
