#pragma once

#include "CommandListener.h"
#include "KafkaW/KafkaW.h"
#include "MainOpt.h"
#include "MasterInterface.h"
#include "StreamMaster.h"
#include <atomic>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

namespace FileWriter {

/// \brief Listens to the Kafka configuration topic and handles any requests.
///
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class Master : public MasterInterface {
public:
  explicit Master(MainOpt &Config);

  /// \brief Sets up command listener and handles any commands received.
  ///
  /// Continues running until stop requested.
  void run() override;

  /// Stop running.
  void stop() override;
  void handle_command_message(
      std::unique_ptr<KafkaW::ConsumerMessage> &&msg) override;
  void handle_command(std::string const &command) override;
  void statistics() override;
  void addStreamMaster(std::unique_ptr<StreamMaster> StreamMaster) override;
  void stopStreamMasters() override;
  std::unique_ptr<StreamMaster> &
  getStreamMasterForJobID(std::string const &JobID) override;
  MainOpt &getMainOpt() override;
  std::shared_ptr<KafkaW::ProducerTopic> getStatusProducer() override;

  /// \brief The unique identifier for this file writer on the network.
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
  std::vector<std::unique_ptr<StreamMaster>> StreamMasters;
  std::string file_writer_process_id_;
  MainOpt &MainConfig;
};

} // namespace FileWriter
