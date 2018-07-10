#pragma once

#include "CommandListener.h"
#include "KafkaW/KafkaW.h"
#include "MainOpt.h"
#include "MasterI.h"
#include "StreamMaster.h"
#include "Streamer.h"
#include <atomic>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

namespace FileWriter {

/// Listens to the Kafka configuration topic and handles any requests.
///
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class Master : public MasterI {
public:
  Master(MainOpt &config);

  /// Sets up command listener and handles any commands received.
  ///
  /// Continues running until stop requested.
  void run() override;

  /// Stop running.
  void stop() override;
  void handle_command_message(std::unique_ptr<KafkaW::Msg> &&msg) override;
  void handle_command(std::string const &command) override;
  void statistics() override;
  void addStreamMaster(
      std::unique_ptr<StreamMaster<Streamer>> StreamMaster) override;
  void stopStreamMasters() override;
  std::unique_ptr<StreamMaster<Streamer>> &
  getStreamMasterForJobID(std::string JobID) override;
  MainOpt &getMainOpt() override;
  std::shared_ptr<KafkaW::ProducerTopic> getStatusProducer() override;

  /// The unique identifier for this file writer on the network.
  ///
  /// \return The unique id.
  std::string file_writer_process_id() const override;

  bool RunLoopExited() override { return HasExitedRunLoop; };
  // std::function<void(void)> cb_on_filewriter_new;
  std::shared_ptr<KafkaW::ProducerTopic> status_producer;

private:
  CommandListener command_listener;
  std::atomic<bool> do_run{true};
  std::atomic<bool> HasExitedRunLoop{false};
  std::vector<std::unique_ptr<StreamMaster<Streamer>>> StreamMasters;
  std::string file_writer_process_id_;
  MainOpt &MainOpt_;
};

} // namespace FileWriter
