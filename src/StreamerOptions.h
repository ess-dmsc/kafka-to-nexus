//===-- src/StreamerOptions.h - Streamer consumer options class definition --*-
// C++ -*-===//
//
//
//===----------------------------------------------------------------------===//
///
/// \file This file contains the declaration of the StreamerOptions class, which
/// is used to configure the Streamer and the RdKafka configuration used int the
/// Streamer. Implementation is in Streamer.cpp
///
//===----------------------------------------------------------------------===//

#pragma once

#include <rapidjson/schema.h>

#include "KafkaW/BrokerSettings.h"
#include <chrono>

namespace FileWriter {

/// Class that contains configuration parameters for the Streamer
class StreamerOptions {

public:
  void setStreamerOptions(const rapidjson::Value *);
  void setRdKafkaOptions(const rapidjson::Value *);
  // private:

  KafkaW::BrokerSettings Settings;
  std::chrono::milliseconds StartTimestamp{0};
  std::chrono::milliseconds StopTimestamp{0};
  std::chrono::milliseconds BeforeStartTime{1000};
  std::chrono::milliseconds AfterStopTime{1000};
  std::chrono::milliseconds ConsumerTimeout{1000};
  int NumMetadataRetry{5};
};

} // namespace FileWriter
