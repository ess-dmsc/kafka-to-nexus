#pragma once
#include "Consumer.h"
#include <memory>

namespace KafkaW {
std::unique_ptr<Consumer> createConsumer(const BrokerSettings &Settings,
                                         const std::string &Broker);
} // namespace KafkaW
