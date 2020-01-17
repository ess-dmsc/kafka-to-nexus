#include "CarbonTestServer.h"
#include <ciso646>
#include <functional>

CarbonTestServer::CarbonTestServer(short port)
    : service(),
      acceptor(service, asio::ip::tcp::endpoint(asio::ip::tcp::v6(), port)) {
  socketError.clear();
  connections = 0;
  receivedBytes = 0;
  WaitForNewConnection();
  acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
  asioThread = std::thread(&CarbonTestServer::ThreadFunction, this);
}

void CarbonTestServer::WaitForNewConnection() {
  sock_ptr cSock(std::make_shared<asio::ip::tcp::socket>(service));
  acceptor.async_accept(*cSock.get(),
                        std::bind(&CarbonTestServer::OnConnectionAccept, this,
                                  std::placeholders::_1, cSock));
}

CarbonTestServer::~CarbonTestServer() {
  CloseAllConnections();
  acceptor.close();
  asioThread.join();
}

void CarbonTestServer::CloseFunction() {
  for (auto sock : existingSockets) {
    if (sock->is_open()) {
      try {
        sock->shutdown(asio::socket_base::shutdown_both);
      } catch (std::exception &e) {
      }
    }
    sock->close();
  }
  existingSockets.clear();
}

void CarbonTestServer::CloseAllConnections() {
  service.post(std::bind(&CarbonTestServer::CloseFunction, this));
}

std::string CarbonTestServer::GetLatestMessage() {
  std::string tempStr = previousMessage;
  previousMessage = "";
  return tempStr;
}

void CarbonTestServer::ThreadFunction() { service.run(); }

void CarbonTestServer::OnConnectionAccept(const std::error_code &ec,
                                          sock_ptr cSock) {
  socketError = ec;
  if (asio::error::basic_errors::operation_aborted == ec or
      asio::error::basic_errors::bad_descriptor == ec) {
    return;
  } else if (ec) {
    return;
  }
  existingSockets.push_back(cSock);
  connections++;
  cSock->async_read_some(asio::buffer(receiveBuffer, bufferSize),
                         std::bind(&CarbonTestServer::HandleRead, this,
                                   std::placeholders::_1, std::placeholders::_2,
                                   cSock));
}

void CarbonTestServer::HandleRead(std::error_code ec, std::size_t bytesReceived,
                                  sock_ptr cSock) {
  socketError = ec;
  if (asio::error::operation_aborted == ec) {
    RemoveSocket(cSock);
    connections--;
    WaitForNewConnection();
    return;
  } else if (ec) {
    RemoveSocket(cSock);
    connections--;
    WaitForNewConnection();
    return;
  }
  receivedBytes += bytesReceived;
  for (size_t j = 0; j < bytesReceived; j++) {
    if ('\0' == receiveBuffer[j]) {
      previousMessage = currentMessage;
      currentMessage = "";
      ++nrOfMessagesReceived;
    } else {
      currentMessage += receiveBuffer[j];
    }
  }
  cSock->async_read_some(asio::buffer(receiveBuffer, bufferSize),
                         std::bind(&CarbonTestServer::HandleRead, this,
                                   std::placeholders::_1, std::placeholders::_2,
                                   cSock));
}

void CarbonTestServer::RemoveSocket(sock_ptr const &cSock) {
  for (size_t i = 0; i < existingSockets.size(); i++) {
    if (existingSockets[i] == cSock) {
      existingSockets.erase(existingSockets.begin() + i);
      return;
    }
  }
}

std::error_code CarbonTestServer::GetLastSocketError() {
  auto tempError = socketError;
  socketError.clear();
  return tempError;
}
