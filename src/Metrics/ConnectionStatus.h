#pragma once

namespace Metrics {
enum struct Status {
  ADDR_LOOKUP,
  ADDR_RETRY_WAIT,
  CONNECT,
  SEND_LOOP,
};
} // namespace Metrics
