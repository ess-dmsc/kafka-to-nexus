//
// Created by Jonas Nilsson on 2019-10-15.
//

#include "WriteMessage.h"


WriteMessage::WriteMessage(DstId DestinationId, FileWriter::FlatbufferMessage const &Msg) : FbMsg(Msg), DestId(DestinationId) {
}