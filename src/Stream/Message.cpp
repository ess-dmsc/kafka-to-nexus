//
// Created by Jonas Nilsson on 2019-10-15.
//

#include "Message.h"

namespace Stream {

Message::Message(WriterModule::Base *DestinationModule,
                 FileWriter::FlatbufferMessage const &Msg)
    : FbMsg(Msg), DestPtr(DestinationModule) {}

} // namespace Stream