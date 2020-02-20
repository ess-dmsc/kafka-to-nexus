//
// Created by Jonas Nilsson on 2019-10-15.
//

#include "Message.h"

namespace Stream {

Message::Message(DstId DestinationId, FileWriter::FlatbufferMessage const &Msg)
    : FbMsg(Msg), DestId(DestinationId) {}

} // namespace Stream