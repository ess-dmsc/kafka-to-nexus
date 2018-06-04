#pragma once

#include "CommandListener.h"
#include "KafkaW/KafkaW.h"
#include "MainOpt.h"
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
class Master {
public:
  Master(MainOpt &config);

  /// Sets up command listener and handles any commands received.
  ///
  /// Continues running until stop requested.
  void run();

  /// Stop running.
  virtual void stop();
  virtual std::vector<std::unique_ptr<StreamMaster<Streamer>>> &
  getStreamMasters() {
    return stream_masters;
  }
  virtual void addStream(std::unique_ptr<StreamMaster<Streamer>> NewStream) {
    stream_masters.emplace_back(std::move(NewStream));
  }
  void handle_command_message(std::unique_ptr<KafkaW::Msg> &&msg);
  void handle_command(std::string const &command);
  std::function<void(void)> cb_on_filewriter_new;
  std::shared_ptr<KafkaW::ProducerTopic> status_producer;
  void statistics();

  /// The unique identifier for this file writer on the network.
  ///
  /// \return The unique id.
  std::string file_writer_process_id() const;

  MainOpt &config;

  bool RunLoopExited() { return HasExitedRunLoop; };

private:
  CommandListener command_listener;
  std::atomic<bool> do_run{true};
  std::atomic<bool> HasExitedRunLoop{false};
  std::vector<std::unique_ptr<StreamMaster<Streamer>>> stream_masters;
  std::string file_writer_process_id_;
};

} // namespace FileWriter
