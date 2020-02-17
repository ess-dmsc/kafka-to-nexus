// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "SourceFilter.h"

namespace Stream {

SourceFilter::SourceFilter(time_point StartTime,
                                         MessageWriter *Destination) : Start(StartTime), Dest(Destination) {

}

void SourceFilter::setStopTime(time_point StopTime) {
  Stop = StopTime;
  StopTimeIsSet = true;
}

bool SourceFilter::hasFinished() {
  return IsDone;
}

void SourceFilter::filterMessage(FileWriter::FlatbufferMessage &&InMsg) {
  if (CMode == TimePoint::BEFORE_WRITE_TIME) {
    if (InMsg.getTimestamp() < Start) {
      if (BufferedMessage.isValid() and BufferedMessage.getTimestamp() < InMsg.getTimestamp()) {
        BufferedMessage = std::move(InMsg);
      }
    } else {
      if (BufferedMessage.isValid()) {
        sendMessage(BufferedMessage);
      }
      sendMessage(InMsg);
      CMode = TimePoint::IS_WRITE_TIME;
    }
  } else if (CMode == TimePoint::IS_WRITE_TIME) {
    sendMessage(InMsg);
    if (StopTimeIsSet and InMsg.getTimestamp() > Stop) {
      CMode = TimePoint::AFTER_WRITE_TIME;
      IsDone = true;
    }
  }
}

} // namespace Stream
