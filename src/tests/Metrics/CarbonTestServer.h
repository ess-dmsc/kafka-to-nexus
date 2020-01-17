#pragma once

#include <asio.hpp>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

using sock_ptr = std::shared_ptr<asio::ip::tcp::socket>;

//------------------------------------------------------------------------------
//     THIS CLASS IS NOT THREAD SAFE AND MAY CRASH AT ANY MOMENT
//------------------------------------------------------------------------------

class CarbonTestServer {
public:
  explicit CarbonTestServer(short port);
  ~CarbonTestServer();
  std::string GetLatestMessage();
  std::error_code GetLastSocketError();
  void CloseAllConnections();
  int GetNrOfConnections() const { return connections; };
  size_t GetReceivedBytes() const { return receivedBytes; };
  int GetNrOfMessages() const { return nrOfMessagesReceived; };

private:
  asio::io_service service;
  void ThreadFunction();
  std::thread asioThread;

  asio::ip::tcp::acceptor acceptor;

  void WaitForNewConnection();
  void OnConnectionAccept(const std::error_code &ec, sock_ptr cSock);
  void HandleRead(std::error_code ec, std::size_t bytesReceived,
                  sock_ptr cSock);

  void RemoveSocket(sock_ptr const &cSock);

  static const int bufferSize = 100;
  char receiveBuffer[bufferSize];
  int nrOfMessagesReceived = 0;

  std::error_code socketError;
  std::atomic_int connections;
  std::atomic_uint receivedBytes;

  std::string currentMessage;
  std::string previousMessage;

  std::vector<sock_ptr> existingSockets;

  void CloseFunction();
};
