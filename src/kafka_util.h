#pragma once

#include <string>
#include <stdexcept>

namespace BrightnESS {
namespace FileWriter {

class BrokerFailure : public std::runtime_error {
public:
BrokerFailure(std::string msg);
};

}
}
