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

#if RAPIDJSON_HAS_STDSTRING == 0
#undef RAPIDJSON_HAS_STDSTRING
#define RAPIDJSON_HAS_STDSTRING 1
#endif

#include "rapidjson/document.h"
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
class JSONWriterBase {
  friend class JSONWriter;
  friend class JSONStreamWriter;

private:
  using ReturnType = rapidjson::Document;

  ReturnType writeImplemented(StreamMasterInfo &) const;
  template <class Allocator>
  rapidjson::Value primaryQuantities(MessageInfo &, Allocator &) const;
  template <class Allocator>
  rapidjson::Value derivedQuantities(MessageInfo &,
                                     const std::chrono::milliseconds &,
                                     Allocator &) const;
};

/// Helper class that give access to the JSON message created in the
/// JSONWriterBase in the form of a rapidjson::Document
class JSONWriter {
public:
  using ReturnType = rapidjson::Document;

  /// Serialize a StreamMasterInfo object into a rapidjson::Document and return
  /// it
  /// \param Informations the StreamMasterInfo object to be serialized
  ReturnType write(StreamMasterInfo &Informations) const;

private:
  /// The main object responsible of serialization
  JSONWriterBase Base;
};

class JSONStreamWriter {
public:
  using ReturnType = std::string;
  /// Serialize a StreamMasterInfo object into a JSON formatted string and
  /// return it
  /// \param Informations the StreamMasterInfo object to be serialized
  ReturnType write(StreamMasterInfo &) const;

private:
  /// The main object responsible of serialization
  JSONWriterBase Base;
};

template <class WriterType>
typename WriterType::ReturnType pprint(StreamMasterInfo &Information) {
  WriterType Writer;
  return Writer.write(Information);
}

} // namespace Status
} // namespace FileWriter
