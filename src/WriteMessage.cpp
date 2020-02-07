//
// Created by Jonas Nilsson on 2019-10-15.
//

#include "WriteMessage.h"


WriteMessage::WriteMessage(DstId DestinationId, FlatbufferMessage const &Msg) : DataPtr(std::make_unique<char[]>(Msg.size())), DataSize(Msg.size()), DestId(DestinationId){
  std::copy(Msg.data(), Msg.data() + Msg.size(), DataPtr.get());
}