#include "kafka_util.h"

namespace BrightnESS {
namespace FileWriter {

BrokerFailure::BrokerFailure(std::string msg) : std::runtime_error(msg) { }

}
}
