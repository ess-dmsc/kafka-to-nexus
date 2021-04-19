#pragma once

#include "CarbonInterface.h"
#include "ConnectionStatus.h"
#include <array>
#include <asio.hpp>
#include <atomic>
#include <memory>
#include <readerwriterqueue/readerwriterqueue.h>
#include <string>
#include <thread>

namespace Metrics {
namespace Carbon {

struct QueryResult;

using MsgQueue = moodycamel::BlockingReaderWriterQueue<std::string>;

class Connection::Impl {
public:
  Impl(std::string Host, int Port);
  virtual ~Impl();
  virtual bool sendMessage(std::string const &Msg) {
    return Messages.try_enqueue(Msg);
  };
  Status getConnectionStatus() const;
  bool messageQueueEmpty() { return Messages.size_approx() == 0; }
  size_t messageQueueSize() { return Messages.size_approx(); }

protected:
  enum struct ReconnectDelay { LONG, SHORT };

  void threadFunction();
  void setState(Status NewState);

  Status ConnectionState{Status::ADDR_LOOKUP};

  std::atomic_bool closeThread{false};

  std::vector<char> MessageBuffer;

  std::string const HostAddress;
  std::string const HostPort;

  std::thread AsioThread;
  const size_t MaxMessageQueueSize{1000};
  MsgQueue Messages{MaxMessageQueueSize};

private:
  size_t const MessageAdditionLimit{3000};
  void resolverHandler(asio::error_code const &Error,
                       asio::ip::tcp::resolver::iterator EndpointIter);
  void connectHandler(asio::error_code const &Error,
                      QueryResult const &AllEndpoints);
  void sentMessageHandler(asio::error_code const &Error, std::size_t BytesSent);
  void receiveHandler(asio::error_code const &Error, std::size_t BytesReceived);
  void trySendMessage();
  void waitForMessage();
  void doAddressQuery();
  void reconnect(ReconnectDelay Delay);
  void tryConnect(QueryResult AllEndpoints);

  using WorkPtr = std::unique_ptr<asio::io_service::work>;

  std::array<std::uint8_t, 64> InputBuffer{};
  asio::io_service Service;
  WorkPtr Work;
  asio::ip::tcp::socket Socket;
  asio::ip::tcp::resolver Resolver;
  asio::system_timer ReconnectTimeout;
};

} // namespace Carbon
} // namespace Metrics
