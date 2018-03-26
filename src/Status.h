//===-- src/Status.h - MessageInfo and StreamMasterInfo class definition
//-------*- C++ -*-===//
//
//
//===----------------------------------------------------------------------===//
///
/// \file
/// This file contains the declaration of the MessageInfo and StreamMasterInfo
/// classes. MessageInfo collects informations about number and the size of
/// messages and the number of errors in a stream. StreamMasterInfo contains a
/// collection of MessageInfo plus other information in order to provide global
/// information about a StreamMaster instance.
///
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include <chrono>
#include <map>

#include "Errors.h"

namespace FileWriter {
namespace Status {

/// \class MessageInfo
/// Stores cumulative information about received messages: number, size (in
/// Megabytes) and number of errors. Assuming a 1-to-1 mapping between Streamer
/// and Topic there will be no concurrent updates of the information, so that
/// members are not required to be atomic. Nevertheless there is concurrency
/// between writes (Streamer) and reads (Report). If no syncronisation mechanism
/// would be present there can be a mixing of updated and non-updated
/// informations, which the mutex allows to avoid.
class MessageInfo {

public:
  MessageInfo() = default;
  MessageInfo(const MessageInfo &Other) = default;
  MessageInfo(MessageInfo &&Other) = default;
  ~MessageInfo() = default;
  MessageInfo &operator=(MessageInfo &&Other) = delete;

  /// Add the information about a new message
  void message(const double &MessageSize);

  /// Increment the error count
  void error();

  /// Reset all the counters
  void reset();

  /// Return the number of megabytes
  /// \return the {MB received, \f$\rm{MB received}^2\f$} pair
  std::pair<double, double> getMbytes() const;

  /// Return the number of messages whose information has been stored
  /// \return the pair {number of messages, number of messages\f$\ 2\f$}
  std::pair<double, double> getMessages() const;

  /// Return the number of errors whose information has been stored
  /// \return the pair {number of messages, number of messages\f$^2\f$}
  double getErrors() const;
  std::mutex &getMutex() { return Mutex; }

private:
  double Messages{0};
  double MessagesSquare{0};
  double Mbytes{0};
  double MbytesSquare{0};
  double Errors{0};
  std::mutex Mutex;
};

/// \class StreamMasterInfo
/// Collect information about each stream using a collection of MessageInfo and
/// reduce the same information to give a global overview of the amount of data
/// that has been processed.
class StreamMasterInfo {

public:
  StreamMasterInfo() : StartTime{std::chrono::system_clock::now()} {}
  StreamMasterInfo(const StreamMasterInfo &) = default;
  StreamMasterInfo(StreamMasterInfo &&) = default;
  ~StreamMasterInfo() = default;
  StreamMasterInfo &operator=(const StreamMasterInfo &) = default;
  StreamMasterInfo &operator=(StreamMasterInfo &&) = default;

  /// Add new information about the stream on the topic \param topic. If a
  /// message info for the topic already exists it's updated, if doesn't exist
  /// it's created with the current values
  void add(MessageInfo &info);

  /// Set the ETA of the next message
  /// \param ToNextMessage std::chrono::milliseconds from the last message to
  /// the next
  void setTimeToNextMessage(const std::chrono::milliseconds &ToNextMessage);
  /// get the time difference between two consecutive status messages
  /// \result std::chrono::milliseconds from the last message to the next
  const std::chrono::milliseconds getTimeToNextMessage();

  /// Get the ETA of the next message
  /// \return ToNextMessage std::chrono::milliseconds from the last message to
  /// the next
  const std::chrono::milliseconds timeToNextMessage();
  /// Return the total execution time
  /// \return std::chrono::milliseconds since the class has been created
  const std::chrono::milliseconds runTime();

  /// Return the number of megabytes
  /// \return the {MB received, \f$\rm{MB received}^2\f$} pair
  std::pair<double, double> getMbytes() const;

  /// Return the number of messages whose information has been stored
  /// \return the pair {number of messages, number of messages\f$\ 2\f$}
  std::pair<double, double> getMessages() const;

  /// Return the number of errors whose information has been stored
  /// \return the pair {number of messages, number of messages\f$^2\f$}
  double getErrors() const;

  StreamMasterError StreamMasterStatus{StreamMasterError::NOT_STARTED()};

private:
  std::pair<double, double> Mbytes{0, 0};
  std::pair<double, double> Messages{0, 0};
  double Errors{0};
  std::chrono::system_clock::time_point StartTime;
  std::chrono::milliseconds MillisecondsToNextMessage{0};
};

//------------------------------------------------------------------------------
/// @brief      Return the average size and relative standard deviation of the
/// number of
/// messages between two reports
///
/// @param[in]  Information  The MessageInfo object that stores the data
///
/// @return     A pair containing {average size, standard deviation}
///
std::pair<double, double> messageSize(const MessageInfo &Information);

//------------------------------------------------------------------------------
/// @brief      Return the frequency of the messages that are consumed
///
/// @param[in]  Information  The MessageInfo object that stores the data
/// @param[in]  Duration     The amount of time between two report
///
/// @return     The number of message consumed per sescond if the amount of time
/// is larger enough, 0 otherwise
///
double messageFrequency(const MessageInfo &Information,
                        const std::chrono::milliseconds &Duration);

//------------------------------------------------------------------------------
/// @brief      Return the throughput of the writer, assuming that each message
/// consumed correctly is written
///
/// @param[in]  Information  The MessageInfo object that stores the data
/// @param[in]  Duration     The amount of time between two report
///
/// @return     The throughput if the amount of time is larger enough, 0
/// otherwise
double messageThroughput(const MessageInfo &Information,
                         const std::chrono::milliseconds &Duration);

} // namespace Status
} // namespace FileWriter
