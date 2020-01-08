#include "CarbonInterface.h"
#include "CarbonConnection.h"

namespace Metrics {

CarbonConnection::CarbonConnection(std::string Host, int Port)
    : Pimpl(std::make_unique<CarbonConnection::Impl>(std::move(Host), Port)) {}

CarbonConnection::~CarbonConnection() = default;

void CarbonConnection::sendMessage(std::string Msg) { Pimpl->sendMessage(Msg); }

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
