// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "KafkaW/MetadataException.h"
#include <chrono>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <set>
#include <string>

namespace KafkaW {
using time_point = std::chrono::system_clock::time_point;
using duration = std::chrono::system_clock::duration;

const RdKafka::TopicMetadata *
findKafkaTopic(const std::string &Topic,
               const RdKafka::Metadata *KafkaMetadata);

template <class KafkaHandle, class KafkaConf>
std::unique_ptr<RdKafka::Handle> getKafkaHandle(std::string Broker) {
  auto Conf =
      std::unique_ptr<KafkaConf>(KafkaConf::create(RdKafka::Conf::CONF_GLOBAL));
  std::string ErrorStr;
  if (Conf->set("metadata.broker.list", Broker, ErrorStr) !=
      RdKafka::Conf::CONF_OK) {
    throw MetadataException("Got error when configuring metadata brokers: \"" +
                            ErrorStr + "\"");
  }
  auto KafkaConsumer = std::unique_ptr<RdKafka::Handle>(
      KafkaHandle::create(Conf.get(), ErrorStr));
  if (KafkaConsumer == nullptr) {
    throw MetadataException("Unable to create kafka handle.");
  }
  return KafkaConsumer;
}

template <class KafkaHandle>
std::vector<std::pair<int, int64_t>>
getOffsetForTimeImpl(std::string Broker, std::string Topic,
                     std::vector<int> Partitions, time_point Time,
                     duration TimeOut) {
  auto Handle = getKafkaHandle<KafkaHandle, RdKafka::Conf>(Broker);
  auto UsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(
                      Time.time_since_epoch())
                      .count();

  std::vector<std::unique_ptr<RdKafka::TopicPartition>> TopicPartitions;
  std::vector<RdKafka::TopicPartition *> TopicPartitionsRaw;
  for (const auto &PartitionId : Partitions) {
    auto CTopicPartition = std::unique_ptr<RdKafka::TopicPartition>(
        RdKafka::TopicPartition::create(Topic, PartitionId, UsedTime));
    TopicPartitionsRaw.emplace_back(CTopicPartition.get());
    TopicPartitions.push_back(std::move(CTopicPartition));
  }

  auto TimeOutInMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimeOut).count();

  auto ReturnCode = Handle->offsetsForTimes(TopicPartitionsRaw, TimeOutInMs);
  if (ReturnCode != RdKafka::ERR_NO_ERROR) {
    throw MetadataException("Failed to query broker for offset corresponding "
                            "to timestamp. Error code was: " +
                            std::to_string(ReturnCode));
  }
  std::vector<std::pair<int, int64_t>> ReturnSet;
  for (const auto &CTopicPartition : TopicPartitions) {
    ReturnSet.emplace_back(std::make_pair(CTopicPartition->partition(),
                                          CTopicPartition->offset()));
  }
  return ReturnSet;
}

template <class KafkaHandle>
std::vector<int> getPartitionsForTopicImpl(std::string Broker, std::string Topic,
                                        duration TimeOut) {
  auto Handle = getKafkaHandle<KafkaHandle, RdKafka::Conf>(Broker);
  std::string ErrorStr;
  auto TopicObj = std::unique_ptr<RdKafka::Topic>(
      RdKafka::Topic::create(Handle.get(), Topic, nullptr, ErrorStr));
  auto TimeOutInMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimeOut).count();
  RdKafka::Metadata *MetadataPtr{nullptr};
  auto ReturnCode =
      Handle->metadata(true, TopicObj.get(), &MetadataPtr, TimeOutInMs);
  if (ReturnCode != RdKafka::ERR_NO_ERROR) {
    throw MetadataException(
        "Failed to query broker for available partitions. Error code was: " +
        std::to_string(ReturnCode));
  }
  auto TopicMetaData = findKafkaTopic(Topic, MetadataPtr);
  std::vector<int> ReturnVector;
  for (auto const &Partition : *TopicMetaData->partitions()) {
    ReturnVector.push_back(Partition->id());
  }
  delete MetadataPtr;
  return ReturnVector;
}

template <class KafkaHandle>
std::set<std::string> getTopicListImpl(std::string Broker, duration TimeOut) {
  auto Handle = getKafkaHandle<KafkaHandle, RdKafka::Conf>(Broker);
  std::string ErrorStr;
  auto TimeOutInMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimeOut).count();
  RdKafka::Metadata *MetadataPtr{nullptr};
  auto ReturnCode = Handle->metadata(true, nullptr, &MetadataPtr, TimeOutInMs);
  if (ReturnCode != RdKafka::ERR_NO_ERROR) {
    throw MetadataException(
        "Failed to query broker for available partitions. Error code was: " +
        std::to_string(ReturnCode));
  }
  auto Topics = MetadataPtr->topics();
  std::set<std::string> TopicNames;
  for (auto const &CTopic : *Topics) {
    TopicNames.emplace(CTopic->topic());
  }
  delete MetadataPtr;
  return TopicNames;
}

} // namespace KafkaW
