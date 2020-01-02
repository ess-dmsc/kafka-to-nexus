/* Copyright (C) 2018 European Spallation Source, ERIC. See LICENSE file */
//===----------------------------------------------------------------------===//
///
/// \file
///
/// \brief Graylog-server interface header file.
///
//===----------------------------------------------------------------------===//

#pragma once

#include "ConnectionStatus.h"
#include <cstdint>
#include <memory>
#include <string>

namespace Metrics {

class CarbonConnection {
public:
  CarbonConnection(std::string Host, int Port);
  virtual ~CarbonConnection();
  virtual void sendMessage(std::string Msg);
  virtual Status getConnectionStatus() const;
  virtual bool messageQueueEmpty();
  virtual size_t messageQueueSize();

private:
  class Impl;
  std::unique_ptr<Impl> Pimpl;
};

} // namespace Metrics
