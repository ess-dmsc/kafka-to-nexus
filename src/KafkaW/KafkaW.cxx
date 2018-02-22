#include "KafkaW.h"
#include "TopicSettings.h"
#include <atomic>
#include <cerrno>

namespace KafkaW {

static_assert(RD_KAFKA_RESP_ERR_NO_ERROR == 0, "We rely on NO_ERROR == 0");
}
