/* Copyright (C) 2018 European Spallation Source, ERIC. See LICENSE file */
//===----------------------------------------------------------------------===//
///
/// \file
///
/// \brief Implements the networking code for sending messages to a graylog
/// server.
///
//===----------------------------------------------------------------------===//

#include "CarbonConnection.h"
#include <chrono>
#include <ciso646>
#include <utility>

namespace {
#define UNUSED_ARG(x) (void)x;
} // namespace

namespace Metrics {

using namespace std::chrono_literals;

struct QueryResult {
  explicit QueryResult(asio::ip::tcp::resolver::iterator &&Endpoints)
      : EndpointIterator(std::move(Endpoints)) {
    while (EndpointIterator != asio::ip::tcp::resolver::iterator()) {
      auto CEndpoint = *EndpointIterator;
      EndpointList.push_back(CEndpoint);
      ++EndpointIterator;
    }
    std::sort(EndpointList.begin(), EndpointList.end(), [](auto &a, auto &b) {
      return a.address().is_v6() < b.address().is_v6();
    });
  }
  asio::ip::tcp::endpoint getNextEndpoint() {
    if (NextEndpoint < EndpointList.size()) {
      return EndpointList[NextEndpoint++];
    }
    return {};
  }
  bool isDone() const { return NextEndpoint >= EndpointList.size(); }
  asio::ip::tcp::resolver::iterator EndpointIterator;
  std::vector<asio::ip::tcp::endpoint> EndpointList;
  size_t NextEndpoint{0};
};

void CarbonConnection::Impl::tryConnect(QueryResult AllEndpoints) {
  asio::ip::tcp::endpoint CurrentEndpoint = AllEndpoints.getNextEndpoint();
  auto HandlerGlue = [this, AllEndpoints](auto &Err) {
    this->connectHandler(Err, AllEndpoints);
  };
  Socket.async_connect(CurrentEndpoint, HandlerGlue);
  setState(Status::CONNECT);
}

CarbonConnection::Impl::Impl(std::string Host, int Port)
    : HostAddress(std::move(Host)), HostPort(std::to_string(Port)), Service(),
      Work(std::make_unique<asio::io_service::work>(Service)), Socket(Service),
      Resolver(Service), ReconnectTimeout(Service, 10s) {
  doAddressQuery();
  AsioThread = std::thread(&CarbonConnection::Impl::threadFunction, this);
}

void CarbonConnection::Impl::resolverHandler(
    const asio::error_code &Error,
    asio::ip::tcp::resolver::iterator EndpointIter) {
  if (Error) {
    setState(Status::ADDR_RETRY_WAIT);
    reConnect(ReconnectDelay::LONG);
    return;
  }
  QueryResult AllEndpoints(std::move(EndpointIter));
  tryConnect(AllEndpoints);
}

void CarbonConnection::Impl::connectHandler(const asio::error_code &Error,
                                            const QueryResult &AllEndpoints) {
  if (!Error) {
    setState(Status::SEND_LOOP);
    auto HandlerGlue = [this](auto &Error, auto Size) {
      this->receiveHandler(Error, Size);
    };
    Socket.async_receive(asio::buffer(InputBuffer), HandlerGlue);
    trySendMessage();
    return;
  }
  Socket.close();
  if (AllEndpoints.isDone()) {
    reConnect(ReconnectDelay::LONG);
    return;
  }
  tryConnect(AllEndpoints);
}

void CarbonConnection::Impl::reConnect(ReconnectDelay Delay) {
  auto HandlerGlue = [this](auto &) { this->doAddressQuery(); };
  switch (Delay) {
  case ReconnectDelay::SHORT:
    ReconnectTimeout.expires_after(100ms);
    break;
  case ReconnectDelay::LONG: // Fallthrough
  default:
    ReconnectTimeout.expires_after(10s);
    break;
  }
  ReconnectTimeout.async_wait(HandlerGlue);
  setState(Status::ADDR_RETRY_WAIT);
}

void CarbonConnection::Impl::receiveHandler(const asio::error_code &Error,
                                            std::size_t BytesReceived) {
  UNUSED_ARG(BytesReceived);
  if (Error) {
    Socket.close();
    reConnect(ReconnectDelay::SHORT);
    return;
  }
  auto HandlerGlue = [this](auto &Error, auto Size) {
    this->receiveHandler(Error, Size);
  };
  Socket.async_receive(asio::buffer(InputBuffer), HandlerGlue);
}

void CarbonConnection::Impl::trySendMessage() {
  if (not Socket.is_open()) {
    return;
  }
  auto HandlerGlue = [this](auto &Err, auto Size) {
    this->sentMessageHandler(Err, Size);
  };
  if (MessageBuffer.size() > MessageAdditionLimit) {
    asio::async_write(Socket, asio::buffer(MessageBuffer), HandlerGlue);
    return;
  }
  std::string NewMessage;
  bool PopResult = Messages.try_dequeue(NewMessage);
  if (PopResult) {
    std::copy(NewMessage.begin(), NewMessage.end(),
              std::back_inserter(MessageBuffer));
    MessageBuffer.push_back('\0');
    asio::async_write(Socket, asio::buffer(MessageBuffer), HandlerGlue);
  } else if (!MessageBuffer.empty()) {
    asio::async_write(Socket, asio::buffer(MessageBuffer), HandlerGlue);
    return;
  } else {
    Service.post([this]() { this->waitForMessage(); });
  }
}

void CarbonConnection::Impl::sentMessageHandler(const asio::error_code &Error,
                                                std::size_t BytesSent) {
  if (BytesSent == MessageBuffer.size()) {
    MessageBuffer.clear();
  } else if (BytesSent > 0) {
    std::vector<char> TempVector;
    std::copy(MessageBuffer.begin() + BytesSent, MessageBuffer.end(),
              std::back_inserter(TempVector));
    MessageBuffer = TempVector;
  }
  if (Error) {
    Socket.close();
    return;
  }
  trySendMessage();
}

void CarbonConnection::Impl::waitForMessage() {
  if (not Socket.is_open()) {
    return;
  }
  if (Messages.peek() != nullptr) {
    trySendMessage();
    return;
  }
  std::this_thread::sleep_for(10ms); // Maybe do async sleep?
  Service.post([this]() { this->waitForMessage(); });
}

void CarbonConnection::Impl::doAddressQuery() {
  setState(Status::ADDR_LOOKUP);
  asio::ip::tcp::resolver::query Query(HostAddress, HostPort);
  auto HandlerGlue = [this](auto &Error, auto EndpointIter) {
    this->resolverHandler(Error, EndpointIter);
  };
  Resolver.async_resolve(Query, HandlerGlue);
}

CarbonConnection::Impl::~Impl() {
  Service.stop();
  AsioThread.join();
  try {
    Socket.close();
  } catch (asio::system_error &) {
    // Do nothing
  }
}

Status CarbonConnection::Impl::getConnectionStatus() const {
  return ConnectionState;
}

void CarbonConnection::Impl::threadFunction() { Service.run(); }

void CarbonConnection::Impl::setState(Status NewState) {
  ConnectionState = NewState;
}

} // namespace Metrics
