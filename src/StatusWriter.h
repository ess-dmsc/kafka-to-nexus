//===-- src/StatusWriter.h - StatusWriter class definition -------*- C++
//-*-===//
//
//
//===----------------------------------------------------------------------===//
///
/// \file This file contains the declaration of the StatusWriter
/// class, which reads the information on the current status of a
/// StreamMaster, such as number of received messages, number of
/// errors and execution time and about each Streamer managed by the
/// StreamMaster such as message frequency and throughput. These
/// information are then serialized as a JSON message.
///
//===----------------------------------------------------------------------===//

#pragma once

#include "json.h"
#include <chrono>

namespace FileWriter {
namespace Status {

class StreamMasterInfo;
class MessageInfo;
} // namespace Status
} // namespace FileWriter

namespace FileWriter {
namespace Status {

class NLWriterBase {
  friend class NLJSONWriter;
  friend class NLJSONStreamWriter;

private:
  NLWriterBase();
  NLWriterBase(const NLWriterBase &) = default;
  NLWriterBase(NLWriterBase &&) = default;
  ~NLWriterBase() = default;

  NLWriterBase &operator=(const NLWriterBase &) = default;
  NLWriterBase &operator=(NLWriterBase &&) = default;

  void setJobId(const std::string &JobId);
  void write(StreamMasterInfo &Information);
  void write(MessageInfo &Information, const std::string &Topic,
             const std::chrono::milliseconds &SinceLastMessage);

  nlohmann::json json;
};

/// Helper class that give access to the JSON message
class NLJSONWriter {
public:
  using ReturnType = nlohmann::json;

  void setJobId(const std::string &JobId) { Base.setJobId(JobId); }
  void write(StreamMasterInfo &Information) { Base.write(Information); }
  void write(MessageInfo &Information, const std::string &Topic,
             const std::chrono::milliseconds &SinceLastMessage) {
    Base.write(Information, Topic, SinceLastMessage);
  }
  ReturnType get() { return Base.json; }

private:
  /// The main object responsible of serialization
  NLWriterBase Base;
};

/// Helper class that give access to the JSON message
class NLJSONStreamWriter {
public:
  using ReturnType = std::string;

  void setJobId(const std::string &JobId) { Base.setJobId(JobId); }
  void write(StreamMasterInfo &Information) { Base.write(Information); }
  void write(MessageInfo &Information, const std::string &Topic,
             const std::chrono::milliseconds &SinceLastMessage) {
    Base.write(Information, Topic, SinceLastMessage);
  }

  ReturnType get() { return Base.json.dump(4); }

private:
  /// The main object responsible of serialization
  NLWriterBase Base;
};

} // namespace Status
} // namespace FileWriter
