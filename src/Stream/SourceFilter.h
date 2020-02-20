// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "FlatbufferMessage.h"
#include "Stream/MessageWriter.h"
#include <chrono>

namespace Stream {

using time_point = std::chrono::system_clock::time_point;

class SourceFilter {
public:
  SourceFilter() = default;
  SourceFilter(time_point StartTime, time_point StopTime,
               MessageWriter *Destination);
  ~SourceFilter();
  void addDestinationId(Message::DstId NewDestination) {
    DestIDs.push_back(NewDestination);
  };

  /// \brief Passes message through filter and sends to writer queue if it
  /// passes.
  ///
  /// \param InMsg The flatbuffer message that is to be filtered.
  /// \return True if message passed the filter. False otherwise.

  bool filterMessage(FileWriter::FlatbufferMessage &&InMsg);
  void setStopTime(time_point StopTime);
  bool hasFinished();

private:
  void sendMessage(FileWriter::FlatbufferMessage const &Msg) {
    // Increase msg counter
    for (auto &CDest : DestIDs) {
      Dest->addMessage({CDest, Msg});
    }
  }
  time_point Start;
  time_point Stop;
  uint64_t CurrentTimeStamp{0};
  MessageWriter *Dest{nullptr};
  bool IsDone{false};
  FileWriter::FlatbufferMessage BufferedMessage;
  std::vector<Message::DstId> DestIDs;
};

} // namespace Stream
