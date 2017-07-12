#include "kafka_util.h"

namespace FileWriter {

BrokerFailure::BrokerFailure(std::string msg) : std::runtime_error(msg) {}

} // namespace FileWriter
