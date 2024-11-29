#include "CarbonConnection.h"
#include "SetThreadName.h"
#include <chrono>
#include <ciso646>
#include <utility>

namespace Metrics::Carbon {

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

void Connection::Impl::tryConnect(QueryResult AllEndpoints) {
  asio::ip::tcp::endpoint CurrentEndpoint = AllEndpoints.getNextEndpoint();
  auto HandlerGlue = [this, AllEndpoints](auto const &Err) {
    this->connectHandler(Err, AllEndpoints);
  };
  Socket.async_connect(CurrentEndpoint, HandlerGlue);
  setState(Status::CONNECT);
}

Connection::Impl::Impl(std::string Host, int Port)
    : HostAddress(std::move(Host)), HostPort(std::to_string(Port)), Service(),
      Work(std::make_unique<asio::io_service::work>(Service)), Socket(Service),
      Resolver(Service), ReconnectTimeout(Service, 10s) {
  doAddressQuery();
  AsioThread = std::thread(&Connection::Impl::threadFunction, this);
}

void Connection::Impl::resolverHandler(
    asio::error_code const &Error,
    asio::ip::tcp::resolver::iterator EndpointIter) {
  if (Error) {
    setState(Status::ADDR_RETRY_WAIT);
    reconnect(ReconnectDelay::LONG);
    return;
  }
  QueryResult AllEndpoints(std::move(EndpointIter));
  tryConnect(AllEndpoints);
}

void Connection::Impl::connectHandler(asio::error_code const &Error,
                                      QueryResult const &AllEndpoints) {
  if (!Error) {
    setState(Status::SEND_LOOP);
    auto HandlerGlue = [this](auto const &Error, auto Size) {
      this->receiveHandler(Error, Size);
    };
    Socket.async_receive(asio::buffer(InputBuffer), HandlerGlue);
    trySendMessage();
    return;
  }
  Socket.close();
  if (AllEndpoints.isDone()) {
    reconnect(ReconnectDelay::LONG);
    return;
  }
  tryConnect(AllEndpoints);
}

void Connection::Impl::reconnect(ReconnectDelay Delay) {
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

void Connection::Impl::receiveHandler(
    asio::error_code const &Error, [[maybe_unused]] std::size_t BytesReceived) {
  if (Error) {
    Socket.close();
    reconnect(ReconnectDelay::SHORT);
    return;
  }
  auto HandlerGlue = [this](auto const &Error, auto Size) {
    this->receiveHandler(Error, Size);
  };
  Socket.async_receive(asio::buffer(InputBuffer), HandlerGlue);
}

void Connection::Impl::trySendMessage() {
  if (!Socket.is_open()) {
    return;
  }
  auto HandlerGlue = [this](auto const &Err, auto Size) {
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
    asio::async_write(Socket, asio::buffer(MessageBuffer), HandlerGlue);
  } else if (!MessageBuffer.empty()) {
    asio::async_write(Socket, asio::buffer(MessageBuffer), HandlerGlue);
    return;
  } else {
    Service.post([this]() { this->waitForMessage(); });
  }
}

void Connection::Impl::sentMessageHandler(asio::error_code const &Error,
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

void Connection::Impl::waitForMessage() {
  if (!Socket.is_open()) {
    return;
  }
  if (Messages.peek() != nullptr) {
    trySendMessage();
    return;
  }
  std::this_thread::sleep_for(10ms); // Maybe do async sleep?
  Service.post([this]() { this->waitForMessage(); });
}

void Connection::Impl::doAddressQuery() {
  setState(Status::ADDR_LOOKUP);
  asio::ip::tcp::resolver::query Query(HostAddress, HostPort);
  auto HandlerGlue = [this](auto const &Error, auto EndpointIter) {
    this->resolverHandler(Error, EndpointIter);
  };
  Resolver.async_resolve(Query, HandlerGlue);
}

Connection::Impl::~Impl() {
  Service.stop();
  AsioThread.join();
  try {
    Socket.close();
  } catch (asio::system_error &) {
    // Do nothing
  }
}

Status Connection::Impl::getConnectionStatus() const { return ConnectionState; }

void Connection::Impl::threadFunction() {
  setThreadName("carbon_connection");
  Service.run();
}

void Connection::Impl::setState(Status NewState) { ConnectionState = NewState; }

} // namespace Metrics::Carbon
