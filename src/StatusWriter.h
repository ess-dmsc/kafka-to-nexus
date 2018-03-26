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

// #if RAPIDJSON_HAS_STDSTRING == 0
// #undef RAPIDJSON_HAS_STDSTRING
// #define RAPIDJSON_HAS_STDSTRING 1
// #endif

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

/// Serialise the content of a StreamMasterInfo object in JSON
/// format. In order to access to the serialised message ont of the
/// two helper classes JSONStreamWriter and JSONWriter must be
/// used. The JSON message contains global information about the
/// StreamMaster as well information about each Streamer. For each
/// Streamer the average message size, the average message frequency
/// and the throughput are computed as well.
//
/// The JSON message has the following form
/// \code
///{
///    "type": "stream_master_status",
///    "next_message_eta_ms": 2000,
///    "stream_master": {
///        "state": "not_started",
///        "messages": 270.0,
///        "Mbytes": 212.2,
///        "errors": 30.0,
///        "runtime": 0
///    },
///    "streamer": {
///        "first-stream": {
///            "status": {
///                "messages": 90.0,
///                "Mbytes": 65.0,
///                "errors": 10.0
///            },
///            "statistics": {
///                "size": {
///                    "average": 0.7,
///                    "stdandard_deviation": 0.5
///                },
///                "frequency": {
///                    "average": 45.0,
///                    "stdandard_deviation": 0.0
///                },
///                "throughput": {
///                    "average": 32.5,
///                    "stdandard_deviation": 0.0
///                }
///            }
///        }
/// }
/// \endcode

class NLWriterBase {
  friend class NLJSONWriter;

private:
  NLWriterBase();
  NLWriterBase(const NLWriterBase &) = default;
  NLWriterBase(NLWriterBase &&) = default;
  ~NLWriterBase() = default;

  NLWriterBase &operator=(const NLWriterBase &) = default;
  NLWriterBase &operator=(NLWriterBase &&) = default;
  void write(StreamMasterInfo &Informations);
  void write(MessageInfo &Informations, const std::string &Topic,
             const std::chrono::milliseconds &SinceLastMessage);

  nlohmann::json json;
};

/// Helper class that give access to the JSON message
class NLJSONWriter {
public:
  using ReturnType = nlohmann::json;

  void write(StreamMasterInfo &Informations);
  void write(MessageInfo &Informations, const std::string &Topic,
             const std::chrono::milliseconds &SinceLastMessage);
  ReturnType get();

private:
  /// The main object responsible of serialization
  NLWriterBase Base;
};

} // namespace Status
} // namespace FileWriter
