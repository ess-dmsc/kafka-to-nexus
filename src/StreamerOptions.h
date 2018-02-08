//===-- src/StreamerOptions.h - Streamer consumer options class definition --*-
// C++ -*-===//
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

#include <rapidjson/schema.h>

#include <chrono>
#include <map>
#include <string>
#include <vector>

class StreamerOptions_Test;

namespace FileWriter {

/// Class that contains configuration parameters for the Streamer
class StreamerOptions {
  friend class Streamer;
  friend class CommandHandler;
  friend class StreamerOptionsTest_Test;

public:
  void setStreamerOptions(const rapidjson::Value *);
  void setRdKafkaOptions(const rapidjson::Value *);
  // private:

  std::vector<std::pair<std::string, std::string>> RdKafkaOptions;
  std::chrono::milliseconds StartTimestamp{0};
  std::chrono::milliseconds StopTimestamp{0};
  std::chrono::milliseconds BeforeStartTime{1000};
  std::chrono::milliseconds AfterStopTime{1000};
  std::chrono::milliseconds ConsumerTimeout{1000};
  int NumMetadataRetry{5};
};

} // namespace FileWriter
