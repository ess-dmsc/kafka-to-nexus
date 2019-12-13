/* Copyright (C) 2018 European Spallation Source, ERIC. See LICENSE file */
//===----------------------------------------------------------------------===//
///
/// \file
///
/// \brief Header file of the networking code.
///
//===----------------------------------------------------------------------===//

#pragma once

#include <readerwriterqueue/readerwriterqueue.h>
#include "ConnectionStatus.h"
#include "CarbonInterface.h"
#include <array>
#include <asio.hpp>
#include <atomic>
#include <memory>
#include <string>
#include <thread>

namespace Metrics {

struct QueryResult;

using MsgQueue = moodycamel::BlockingReaderWriterQueue<std::string>;

class CarbonConnection::Impl {
public:
  Impl(std::string Host, int Port);
  virtual ~Impl();
  virtual bool sendMessage(std::string Msg) { return Messages.try_enqueue(Msg); };
  Status getConnectionStatus() const;
  bool messageQueueEmpty() { return Messages.size_approx() == 0; }
  size_t messageQueueSize() { return Messages.size_approx(); }

protected:
  enum class ReconnectDelay { LONG, SHORT };

  void threadFunction();
  void setState(Status NewState);

  Status ConnectionState{Status::ADDR_LOOKUP};

  std::atomic_bool closeThread{false};

  std::vector<char> MessageBuffer;

  std::string HostAddress;
  std::string HostPort;

  std::thread AsioThread;
  MsgQueue Messages;

private:
  const size_t MessageAdditionLimit{3000};
  void resolverHandler(const asio::error_code &Error,
                       asio::ip::tcp::resolver::iterator EndpointIter);
  void connectHandler(const asio::error_code &Error,
                      const QueryResult &AllEndpoints);
  void sentMessageHandler(const asio::error_code &Error, std::size_t BytesSent);
  void receiveHandler(const asio::error_code &Error, std::size_t BytesReceived);
  void trySendMessage();
  void waitForMessage();
  void doAddressQuery();
  void reConnect(ReconnectDelay Delay);
  void tryConnect(QueryResult AllEndpoints);

  typedef std::unique_ptr<asio::io_service::work> WorkPtr;

  std::array<std::uint8_t, 64> InputBuffer{};
  asio::io_service Service;
  WorkPtr Work;
  asio::ip::tcp::socket Socket;
  asio::ip::tcp::resolver Resolver;
  asio::system_timer ReconnectTimeout;
};

} // namespace Metrics
