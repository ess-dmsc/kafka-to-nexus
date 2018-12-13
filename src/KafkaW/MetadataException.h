#pragma once

#include <exception>
#include <string>

class MetadataException : public std::exception {
public:
  explicit MetadataException(std::string Message) : Message(Message) {}
  const char *what() const noexcept override { return Message.c_str(); }

private:
  std::string Message;
};