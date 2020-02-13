// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "ThreadedExecutor.h"
#include "DataMessageWriter.h"
#include "FlatbufferMessage.h"
#include "WriteMessage.h"
#include <chrono>
#include "KafkaW/Consumer.h"


// Pollution of namespace, fix.
using SrcToDst = std::vector<std::pair<FileWriter::FlatbufferMessage::SrcHash, WriteMessage::DstId>>;
using std::chrono_literals::operator ""ms;
using time_point = std::chrono::system_clock::time_point;

class PartitionStream {
public:
  PartitionStream() = default;
  PartitionStream(std::unique_ptr<KafkaW::Consumer> Consumer, SrcToDst Map, Metrics::Registrar RegisterMetric, time_point Stop = time_point::max());
  void setStopTime(time_point Stop);
  bool hasFinished();
protected:
  Metrics::Metric KafkaTimeouts{"timeouts", "Timeouts when polling for messages."};
  Metrics::Metric KafkaErrors{"errors", "Errors received when polling for messages.", Metrics::Severity::ERROR};
  Metrics::Metric MessagesReceived{"received", "Number of messages received from broker."};
  Metrics::Metric MessagesProcessed{"processed", "Number of messages queued up for writing."};
  Metrics::Metric BadOffsets{"bad_offsets", "Number of messages received with bad offsets."};
  Metrics::Metric BadTimestamps{"bad_timestamps", "Number of messages received with bad timestamps."};

  void pollForMessage();
  std::unique_ptr<KafkaW::Consumer> ConsumerPtr;
  std::atomic_bool HasFinished{false};
  SrcToDst DataMap;
  time_point StopTime;
  std::int64_t CurrentOffset{0};
  ThreadedExecutor Executor; //Must be last
};
