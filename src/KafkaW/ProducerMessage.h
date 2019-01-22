#pragma once
#include <stdint.h>

namespace KafkaW {
    struct ProducerMessage {
        virtual ~ProducerMessage() = default;
        unsigned char *data;
        uint32_t size;
    };
}
