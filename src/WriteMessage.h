//
// Created by Jonas Nilsson on 2019-10-15.
//

#pragma once

#include "FlatbufferMessage.h"
#include <memory>

class WriteMessage {
public:
  using DstId = intptr_t;
  WriteMessage() = default;
  WriteMessage(DstId DestinationId, FileWriter::FlatbufferMessage const &Msg);
  FileWriter::FlatbufferMessage FbMsg;
  DstId DestId{0};
};
