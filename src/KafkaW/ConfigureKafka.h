#pragma once

#include "BrokerSettings.h"
#include <librdkafka/rdkafkacpp.h>

namespace KafkaW {
void configureKafka(RdKafka::Conf *RdKafkaConfiguration,
                    KafkaW::BrokerSettings Settings);
}