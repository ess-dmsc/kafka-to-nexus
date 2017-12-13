//===-- src/StreamerOptions.h - Streamer consumer options class definition --*- C++ -*-===//
//
//
//===----------------------------------------------------------------------===//
///
/// \file This file contains the declaration of the StreamerOptions class, which
/// is used to configure the Streamer and the RdKafka configuration used int the
/// Streamer. Implementation is in Streamer.cxx
///
//===----------------------------------------------------------------------===//

#pragma once

#include "utils.h"

#include <rapidjson/schema.h>

namespace FileWriter {

/// Class that contains configuration parameters for the Streamer
class StreamerOptions {
  friend class Streamer;
  friend class CommandHandler;
public:

  void SetStreamerOptions(const rapidjson::Value*);
  void SetRdKafkaOptions(const rapidjson::Value*);  
private:
  
  std::vector<std::pair<std::string,std::string>> RdKafkaOptions;
  milliseconds BeforeStartTime{1000};
  milliseconds StartTimestamp{0};
  milliseconds ConsumerTimeout{1000};
  int NumMetadataRetry{5};
};


} // namespace FileWriter
