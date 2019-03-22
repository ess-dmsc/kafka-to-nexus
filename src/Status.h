/// \file
/// MessageInfo collects information about number and the size of
/// messages and the number of errors in a stream.
/// StreamMasterInfo contains a collection of MessageInfo plus other
/// information in order to provide global information about a
/// StreamMaster instance.

#pragma once

#include <chrono>
#include <mutex>

#include "Errors.h"

namespace FileWriter {
namespace Status {

/// \brief Stores cumulative information about received messages: number, size
/// (in Megabytes) and number of errors.
///
/// Assuming a 1-to-1 mapping between Streamer and Topic there will
/// be no concurrent updates of the information, so that members are
/// not required to be atomic. Nevertheless there is concurrency
/// between writes (Streamer) and reads (Report). If no synchronisation
/// mechanism would be present there can be a mixing of updated and
/// non-updated information, which the mutex allows to avoid.
class MessageInfo {

public:
  /// \brief Increments the number of messages that have been correctly
  /// processed by one unit and the number of processed megabytes accordingly.
  ///
  /// \param MessageSize The message size in bytes.
  void newMessage(double MessageBytes);

  ///  Increments the error count by one unit.
  void error();

  ///  Reset the message statistics counters.
  void resetStatistics();

  /// \brief Return the average size and relative standard deviation of the
  /// number of messages.
  ///
  /// \return A pair containing {average size, standard deviation}.
  std::pair<double, double> messageSizeStats() const;

  /// \brief Returns the number of megabytes processed.
  ///
  /// \return The number of megabytes.
  double getMbytes() const;

  /// \brief Returns the number of messages that have been processed
  /// correctly.
  ///
  /// \return The number of messages.
  uint64_t getNumberMessages() const;

  /// \brief Returns the number of messages recognised as error.
  ///
  /// \return The number of messages.
  uint64_t getErrors() const;

private:
  uint64_t Messages{0};
  uint64_t Errors{0};
  double Mbytes{0};
  double MbytesSquare{0};
  std::mutex Mutex;
};

/// \brief Collect information about each stream's messages and
/// reduce this information to give a global overview of the
/// amount of data that has been processed.
class StreamMasterInfo {

public:
  StreamMasterInfo() : StartTime{std::chrono::system_clock::now()} {}
  StreamMasterInfo(const StreamMasterInfo &) = default;
  StreamMasterInfo(StreamMasterInfo &&) = default;
  ~StreamMasterInfo() = default;
  StreamMasterInfo &operator=(const StreamMasterInfo &) = default;
  StreamMasterInfo &operator=(StreamMasterInfo &&) = default;

  /// \brief Adds the information collected for a stream.
  ///
  /// \param Info The message information.
  void add(MessageInfo &Info);

  /// \brief Sets the estimate time to next message.
  ///
  /// The next message is expected to arrive at [time of last message] +
  /// [ToNextMessage].
  ///
  /// \param ToNextMessage Time to next message.
  void setTimeToNextMessage(const std::chrono::milliseconds &ToNextMessage);

  /// \brief Get the time difference between two consecutive status messages.
  ///
  /// \return Time from the last message to the next.
  const std::chrono::milliseconds getTimeToNextMessage() const;

  /// \brief Returns the total execution time.
  ///
  /// \return Time since the write command has been issued.
  const std::chrono::milliseconds runTime() const;

  /// \brief Returns the total number of megabytes processed for the
  /// current file.
  ///
  /// \return The number of megabytes.
  double getMbytes() const;

  /// \brief Return the number of messages whose information has been stored.
  ///
  /// \return The number of messages.
  uint64_t getMessages() const;

  /// \brief  Returns the total number of error in the messages processed
  /// for the current file.
  ///
  /// \return  The number of errors.
  uint64_t getErrors() const;

  /// \brief Returns the error status of the StreamMaster associated with the
  /// file.
  ///
  /// \return The StreamMaster status.
  StreamMasterError StreamMasterStatus{StreamMasterError::NOT_STARTED};

private:
  double Mbytes{0};
  uint64_t Messages{0};
  uint64_t Errors{0};
  std::chrono::system_clock::time_point StartTime;
  std::chrono::milliseconds MillisecondsToNextMessage{0};
};

} // namespace Status
} // namespace FileWriter
