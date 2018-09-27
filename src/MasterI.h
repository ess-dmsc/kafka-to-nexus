#pragma once

#include "CommandListener.h"
#include "KafkaW/KafkaW.h"
#include "MainOpt.h"
#include <memory>
#include <string>

namespace FileWriter {

class StreamMaster;

/// Listens to the Kafka configuration topic and handles any requests.
///
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class MasterI {
public:
  /// Sets up command listener and handles any commands received.
  /// Continues running until stop requested.
  virtual void run() = 0;
  virtual void stop() = 0;
  virtual void handle_command_message(std::unique_ptr<KafkaW::Msg> &&msg) = 0;
  virtual void handle_command(std::string const &command) = 0;
  virtual void statistics() = 0;

  /// The unique identifier for this file writer on the network.
  ///
  /// \return The unique id.
  virtual std::string file_writer_process_id() const = 0;

  virtual bool RunLoopExited() = 0;
  virtual MainOpt &getMainOpt() = 0;
  virtual std::shared_ptr<KafkaW::ProducerTopic> getStatusProducer() = 0;
  virtual void addStreamMaster(std::unique_ptr<StreamMaster> StreamMaster) = 0;
  virtual void stopStreamMasters() = 0;
  virtual std::unique_ptr<StreamMaster> &
  getStreamMasterForJobID(std::string JobID) = 0;
};

} // namespace FileWriter
