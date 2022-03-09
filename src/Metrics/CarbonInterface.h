#pragma once

#include "ConnectionStatus.h"
#include <cstdint>
#include <memory>
#include <string>

namespace Metrics::Carbon {

/// \brief Used for sending updates to the Grafana (Carbon) service in use.
class Connection {
public:
  Connection(std::string Host, int Port);
  virtual ~Connection();
  virtual void sendMessage(std::string const &Msg);
  virtual Status getConnectionStatus() const;
  virtual bool messageQueueEmpty();
  virtual size_t messageQueueSize();

private:
  class Impl;
  std::unique_ptr<Impl> Pimpl;
};
} // namespace Metrics::Carbon
