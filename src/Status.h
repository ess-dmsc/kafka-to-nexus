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

#include <map>

#include "Errors.h"
#include "utils.h"

namespace FileWriter {
namespace Status {

/// \class MessageInfo
/// Stores cumulative information about received messages: number, size (in
/// Megabytes) and number of errors
class MessageInfo {

public:
  using value_type = std::pair<double, double>;
  /// Implement the = operator for MessageInfo.
  const MessageInfo &operator=(const MessageInfo &Info);

  /// Implement the += operator for MessageInfo using atomic_add
  const MessageInfo &operator+=(const MessageInfo &Info);

  /// Add the information about a new message
  const MessageInfo &message(const double &MessageSize);

  /// Increment the error count
  const MessageInfo &error();

  /// Reset all the counters
  void reset();

  /// Return the number of megabytes
  /// \return the {MB received, \f$\rm{MB received}^2\f$} pair
  value_type getMbytes() const;

  /// Return the number of messages whose information has been stored
  /// \return the pair {number of messages, number of messages\f$\ 2\f$}
  value_type getMessages() const;

  /// Return the number of errors whose information has been stored
  /// \return the pair {number of messages, number of messages\f$^2\f$}
  double getErrors() const;
  std::mutex &getMutex() { return Mutex; }

private:
  std::atomic<double> Messages{0};
  std::atomic<double> MessagesSquare{0};
  std::atomic<double> Mbytes{0};
  std::atomic<double> MbytesSquare{0};
  std::atomic<double> Errors{0};
  std::mutex Mutex;
};

/// \class StreamMasterInfo
/// Collect information about each stream using a collection of MessageInfo and
/// reduce the same information to give a global overview of the amount of data
/// that has been processed.
class StreamMasterInfo {
  using SMEC = StreamMasterErrorCode;

public:
  StreamMasterInfo() : StartTime{std::chrono::system_clock::now()} {}

  /// Add new information about the stream on the topic \param topic. If a
  /// message info for the topic already exists it's updated, if doesn't exist
  /// it's created with the current values
  void add(const std::string &topic, MessageInfo &info);

  /// Return the whole collection of information.
  /// \return a map <topic name, messages information>
  std::map<std::string, MessageInfo> &info() { return StreamsInfo; }

  /// Return the cumulative information.
  /// \return a message info where all the messages information are accumulated
  MessageInfo &getTotal() { return Total; }

  /// Set the status
  SMEC &status(const SMEC &other) {
    Status = other;
    return Status;
  }

  /// Return the registered status.
  SMEC &status() { return Status; }

  /// Set the ETA of the next message
  /// \param ToNextMessage milliseconds from the last message to the next
  void setTimeToNextMessage(const milliseconds &ToNextMessage);
  /// get the time difference between two consecutive status messages
  /// \result milliseconds from the last message to the next
  const milliseconds getTimeToNextMessage();

  /// Get the ETA of the next message
  /// \return ToNextMessage milliseconds from the last message to the next
  const milliseconds timeToNextMessage();
  /// Return the total execution time
  /// \return milliseconds since the class has been created
  const milliseconds runTime();

private:
  MessageInfo Total;
  std::map<std::string, MessageInfo> StreamsInfo;
  std::chrono::system_clock::time_point StartTime;
  milliseconds NextMessageRelativeEta;
  SMEC Status{SMEC::not_started};
};

/// Return the average size and relative standard deviation of the number of
/// messages \param Value the MessageInfo object that stores the data \return
/// the {average size, standard deviation} pair
const std::pair<double, double> messageSize(const MessageInfo &Information);

/// Return the frequency and the relative standard deviation of the number of
/// messages \param value the MessageInfo object that stores the data \return
/// the {frequency, standard deviation} pair
const std::pair<double, double> messageFrequency(const MessageInfo &Information,
                                                 const milliseconds &Duration);

/// Return the throughput and the relative standard deviation of the number of
/// messages \param value the MessageInfo object that stores the data \return
/// the {throughput, standard deviation} pair
const std::pair<double, double>
messageThroughput(const MessageInfo &Information, const milliseconds &Duration);

} // namespace Status
} // namespace FileWriter
