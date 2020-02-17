// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <chrono>
#include "FlatbufferMessage.h"
#include "Stream/MessageWriter.h"

namespace Stream {

using time_point = std::chrono::system_clock::time_point;

class SourceFilter {
public:
  SourceFilter() = default;
  SourceFilter(time_point StartTime, MessageWriter *Destination);
  void addDestinationId(Message::DstId NewDestination) {
    DestIDs.push_back(NewDestination);
  };
  void filterMessage(FileWriter::FlatbufferMessage &&InMsg);
  void setStopTime(time_point StopTime);
  bool hasFinished();
private:
  void sendMessage(FileWriter::FlatbufferMessage const &Msg) {
    for (auto &CDest : DestIDs) {
      Dest->addMessage({CDest, Msg});
    }
  }
  enum class TimePoint {
    BEFORE_WRITE_TIME,
    IS_WRITE_TIME,
    AFTER_WRITE_TIME
  } CMode{TimePoint::BEFORE_WRITE_TIME};
  time_point Start;
  time_point Stop;
  MessageWriter *Dest{nullptr};
  bool IsDone{false};
  bool StopTimeIsSet{false};
  FileWriter::FlatbufferMessage BufferedMessage;
  std::vector<Message::DstId> DestIDs;
};

} // namespace Stream

