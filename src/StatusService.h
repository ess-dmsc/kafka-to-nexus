

#pragma once

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>
#include <cstring>
#include <thread>


namespace Status {
class StatusReporterBase;
}

namespace FileWriter {

class StatusService {
public:
  StatusService(int TcpPort);
  void startThread();
  void run();

private:
  std::thread status;
  int TcpPort{8888};
  int ListenFd{0};
  int ConnFd{0};
  struct sockaddr_in ServerAddr;
  char TxBuffer[1025];
  static constexpr int ONE_SECOND{1};
  static constexpr int MESSAGE_BACKLOG{10};
};
} // namespace FileWriter
