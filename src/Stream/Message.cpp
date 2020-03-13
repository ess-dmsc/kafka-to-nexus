// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Message.h"

namespace Stream {

Message::Message(WriterModule::Base *DestinationModule,
                 FileWriter::FlatbufferMessage const &Msg)
    : FbMsg(Msg), DestPtr(DestinationModule) {}

} // namespace Stream