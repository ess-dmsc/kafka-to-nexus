//
// Created by Jonas Nilsson on 2019-10-15.
//

#pragma once

#include <memory>
#include "FlatbufferMessage.h"

class WriteMessage {
public:
  using DstId = intptr_t;
  WriteMessage() = default;
  WriteMessage(DstId DestinationId, FileWriter::FlatbufferMessage const &Msg);
  FileWriter::FlatbufferMessage FbMsg;
  DstId DestId{0};
};
