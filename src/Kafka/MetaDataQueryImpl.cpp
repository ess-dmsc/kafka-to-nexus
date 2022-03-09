// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Kafka/MetaDataQueryImpl.h"
#include <algorithm>
#include <fmt/format.h>

namespace Kafka {
const RdKafka::TopicMetadata *
findKafkaTopic(const std::string &Topic,
               const RdKafka::Metadata *KafkaMetadata) {
  const auto *Topics = KafkaMetadata->topics();
  auto Iterator =
      std::find_if(Topics->cbegin(), Topics->cend(),
                   [Topic](const RdKafka::TopicMetadata *TopicMetadata) {
                     return TopicMetadata->topic() == Topic;
                   });
  if (Iterator == Topics->end()) {
    throw MetadataException(
        fmt::format(R"(Topic "{}" not listed by broker.)", Topic));
  }
  return *Iterator;
}
} // namespace Kafka
