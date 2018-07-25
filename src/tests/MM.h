#pragma once

#include "MasterI.h"
#include "StreamMaster.h"
#include "Streamer.h"

class MockMasterI : public FileWriter::MasterI {

public:
  void run() {}
  void stop() {}
  void handle_command_message(std::unique_ptr<KafkaW::Msg> &&msg) {}
  void handle_command(std::string const &command) {}
  void statistics(){};
  std::string file_writer_process_id() const { return ProcessId; }
  bool RunLoopExited() { return false; }
  MainOpt &getMainOpt() { return MainOptInst; }
  std::shared_ptr<KafkaW::ProducerTopic> getStatusProducer() { return nullptr; }

  void addStreamMaster(
      std::unique_ptr<FileWriter::StreamMaster<FileWriter::Streamer>>) {}
  void stopStreamMasters() {}
  std::unique_ptr<FileWriter::StreamMaster<FileWriter::Streamer>> &
  getStreamMasterForJobID(std::string JobID_) {
    // if (JobID == JobID_) {
    // }
    static std::unique_ptr<FileWriter::StreamMaster<FileWriter::Streamer>>
        NotFound;
    return NotFound;
  }

private:
  MainOpt MainOptInst;
  std::string ProcessId;
};
