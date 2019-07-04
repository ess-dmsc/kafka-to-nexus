#pragma once

#include <stdexcept>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

/// To signalise errors in JSON command
class UnexpectedJsonInput : public std::runtime_error {
public:
  UnexpectedJsonInput() : std::runtime_error("UnexpectedJsonInput") {}
  explicit UnexpectedJsonInput(const std::string &Error)
      : std::runtime_error(Error) {}
};
} // namespace hs00
} // namespace Schemas
} // namespace FileWriter
