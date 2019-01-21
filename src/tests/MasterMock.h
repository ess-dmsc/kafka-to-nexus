#pragma once

#include "MasterInterface.h"
#include "StreamMaster.h"
#include "Streamer.h"

class MockMasterI : public FileWriter::MasterInterface {

public:
  void run() override {}
  void stop() override {}
  void handle_command_message(
      std::unique_ptr<KafkaW::ConsumerMessage> &&msg) override {}
  void handle_command(std::string const &command) override {}
  void statistics() override{};
  /// \todo Switch out code when clang-tidy merge is done
  std::string file_writer_process_id() const override { return ProcessId; }
  std::string getFileWriterProcessId() const { return ProcessId; }
  //  std::string getFileWriterProcessId() const override { return ProcessId; }
  bool RunLoopExited() override { return false; }
  MainOpt &getMainOpt() override { return MainOptInst; }
  std::shared_ptr<KafkaW::ProducerTopic> getStatusProducer() override {
    return nullptr;
  }

  void addStreamMaster(
      std::unique_ptr<FileWriter::StreamMaster>)
      override {}
  void stopStreamMasters() override {}
  std::unique_ptr<FileWriter::StreamMaster> &
  getStreamMasterForJobID(std::string const &) override {
    static std::unique_ptr<FileWriter::StreamMaster>
        NotFound;
    return NotFound;
  }

private:
  MainOpt MainOptInst;
  std::string ProcessId;
};
