// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

namespace Kafka {

// Provides statistics for all producers
struct ProducerStats {
  std::atomic<uint64_t> produced{0};
  std::atomic<uint32_t> produce_fail{0};
  std::atomic<uint32_t> local_queue_full{0};
  std::atomic<uint64_t> produce_cb{0};
  std::atomic<uint64_t> produce_cb_fail{0};
  std::atomic<uint64_t> poll_served{0};
  std::atomic<uint64_t> msg_too_large{0};
  std::atomic<uint64_t> produced_bytes{0};
  std::atomic<uint32_t> out_queue{0};
  ProducerStats() = default;
  ProducerStats(ProducerStats const &x) {
    // cppcheck-suppress useInitializationList
    produced = x.produced.load();
    produce_fail = x.produce_fail.load();
    local_queue_full = x.local_queue_full.load();
    produce_cb = x.produce_cb.load();
    produce_cb_fail = x.produce_cb_fail.load();
    poll_served = x.poll_served.load();
    msg_too_large = x.msg_too_large.load();
    produced_bytes = x.produced_bytes.load();
    out_queue = x.out_queue.load();
  };
};
} // namespace Kafka
