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
#include <memory>

namespace WriterModule {
class Base;
}

namespace Stream {

class Message {
public:
  using DestPtrType = WriterModule::Base *;
  Message() = default;

  Message(WriterModule::Base *DestinationModule,
  FileWriter::FlatbufferMessage const &Msg)
  : FbMsg(Msg), DestPtr(DestinationModule) {}

  FileWriter::FlatbufferMessage FbMsg;
  DestPtrType DestPtr{nullptr};
};

} // namespace Stream
