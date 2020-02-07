// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \brief Do the actual file writing.
///

#include "DataMessageWriter.h"
#include "WriterModuleBase.h"

void DataMessageWriter::addMessage(WriteMessage Msg) {
  Executor.SendWork([=](){
    writeMsgImpl(Msg.DestId, Msg.FbMsg);
  });
}

void DataMessageWriter::writeMsgImpl(intptr_t ModulePtr, FileWriter::FlatbufferMessage const &Msg) {
  try {
    reinterpret_cast<WriterModule::Base*>(ModulePtr)->write(Msg);
  } catch (WriterModule::WriterException &E) {

  }
}