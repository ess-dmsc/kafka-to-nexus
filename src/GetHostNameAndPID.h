#pragma once

static std::string getHostName() {
  size_t const MaxHostNameLength = 20;
  char Hostname[MaxHostNameLength];
  if (gethostname(Hostname, MaxHostNameLength)) {
    return "UnknownHostname";
  }
  return {Hostname};
}

static uint32_t getPID() { return static_cast<uint32_t>(getpid()); }
