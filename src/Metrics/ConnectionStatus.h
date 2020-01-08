#pragma once

namespace Metrics {
namespace Carbon {
enum struct Status {
  ADDR_LOOKUP,
  ADDR_RETRY_WAIT,
  CONNECT,
  SEND_LOOP,
};
} // namespace Carbon
} // namespace Metrics
