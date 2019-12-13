/* Copyright (C) 2018 European Spallation Source, ERIC. See LICENSE file */
//===----------------------------------------------------------------------===//
///
/// \file
///
/// \brief The interface implementation for sending messages to a graylog
/// server.
///
//===----------------------------------------------------------------------===//

#include "CarbonInterface.h"
#include "CarbonConnection.h"

namespace Metrics {

CarbonConnection::CarbonConnection(std::string Host, int Port)
    : Pimpl(std::make_unique<CarbonConnection::Impl>(std::move(Host), Port)) {}

void CarbonConnection::sendMessage(std::string Msg) {
  Pimpl->sendMessage(Msg);
}

Status CarbonConnection::getConnectionStatus() const {
  return Pimpl->getConnectionStatus();
}

bool CarbonConnection::messageQueueEmpty() {
  return Pimpl->messageQueueEmpty();
}

size_t CarbonConnection::messageQueueSize() {
  return Pimpl->messageQueueSize();
}

} // namespace Metrics
