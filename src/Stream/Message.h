//
// Created by Jonas Nilsson on 2019-10-15.
//

#pragma once

#include "FlatbufferMessage.h"
#include <memory>

namespace Stream {

class Message {
public:
  using DstId = intptr_t;

  Message() = default;

  Message(DstId DestinationId, FileWriter::FlatbufferMessage const &Msg);

  FileWriter::FlatbufferMessage FbMsg;
  DstId DestId{0};
};

} //namespace Stream
