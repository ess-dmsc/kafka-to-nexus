#pragma once

#include <stdexcept>
#include <string>

namespace FileWriter {

class BrokerFailure : public std::runtime_error {
public:
  BrokerFailure(std::string msg);
};

} // namespace FileWriter
