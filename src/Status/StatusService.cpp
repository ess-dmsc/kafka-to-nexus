// Copyright (C) 2023 European Spallation Source, ERIC. See LICENSE file
//===----------------------------------------------------------------------===//
///
/// \file
///
/// \brief StatusService class implementation
///
//===----------------------------------------------------------------------===//

#include <Status/StatusService.h>

namespace Status {

StatusService::StatusService(int Port) : TcpPort(Port) {
  ListenFd = socket(AF_INET, SOCK_STREAM, 0);
  memset(&ServerAddr, 0, sizeof(ServerAddr));
  memset(TxBuffer, 0, sizeof(TxBuffer));

  ServerAddr.sin_family = AF_INET;
  ServerAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  ServerAddr.sin_port = htons(TcpPort);

  bind(ListenFd, (struct sockaddr *)&ServerAddr, sizeof(ServerAddr));

  listen(ListenFd, MESSAGE_BACKLOG);
}

void StatusService::startThread() {
  status = std::thread(&StatusService::run, this);
}

/// \todo can add responses to queries or add runtime
/// data to the returned status message
void StatusService::run() {
  while (true) {
    int ConnFd = accept(ListenFd, (struct sockaddr *)NULL, NULL);
    snprintf(TxBuffer, sizeof(TxBuffer), "STATUS: running\n");
    write(ConnFd, TxBuffer, strlen(TxBuffer));
    close(ConnFd);
    sleep(ONE_SECOND);
  }
}

} // namespace FileWriter
