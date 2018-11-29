#pragma once

#include <stdexcept>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

/// Some JSON input was not understood but we can continue.
class UnexpectedJsonInput : public std::runtime_error {
public:
  UnexpectedJsonInput() : std::runtime_error("UnexpectedJsonInput") {}
};

std::runtime_error unimplemented();
}
}
}
