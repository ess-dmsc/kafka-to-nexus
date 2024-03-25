// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Kafka/MetaDataQuery.h"
#include "Kafka/MetadataException.h"

namespace Kafka {

const RdKafka::TopicMetadata *
findTopicMetadata(const std::string &Topic,
                  const RdKafka::Metadata *KafkaMetadata) {
  return MetadataEnquirer().findKafkaTopic(Topic, KafkaMetadata);
}
} // namespace Kafka
