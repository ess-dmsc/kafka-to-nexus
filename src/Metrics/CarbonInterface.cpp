#include "CarbonInterface.h"
#include "CarbonConnection.h"

namespace Metrics::Carbon {

Connection::Connection(std::string Host, int Port)
    : Pimpl(std::make_unique<Connection::Impl>(std::move(Host), Port)) {}

Connection::~Connection() = default;

void Connection::sendMessage(std::string const &Msg) {
  Pimpl->sendMessage(Msg);
}

Status Connection::getConnectionStatus() const {
  return Pimpl->getConnectionStatus();
}

bool Connection::messageQueueEmpty() { return Pimpl->messageQueueEmpty(); }

size_t Connection::messageQueueSize() { return Pimpl->messageQueueSize(); }

} // namespace Metrics
