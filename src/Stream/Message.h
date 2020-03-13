//
// Created by Jonas Nilsson on 2019-10-15.
//

#pragma once

#include "FlatbufferMessage.h"
#include <memory>

namespace WriterModule {
class Base;
}

namespace Stream {

class Message {
public:
  using DestPtrType = WriterModule::Base *;
  Message() = default;

  Message(DestPtrType DestinationModule,
          FileWriter::FlatbufferMessage const &Msg);

  FileWriter::FlatbufferMessage FbMsg;
  DestPtrType DestPtr{nullptr};
};

} // namespace Stream
