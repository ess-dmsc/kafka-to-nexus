// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Kafka/BrokerSettings.h"
#include "Kafka/MetadataException.h"
#include "TimeUtility.h"
#include <chrono>
#include <fmt/format.h>
#include <librdkafka/rdkafkacpp.h>
#include <set>
#include <string>

namespace Kafka {

class MetadataEnquirer {
public:
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

  template <class KafkaHandle, class KafkaConf>
  std::unique_ptr<RdKafka::Handle>
  getKafkaHandle(std::string Broker, BrokerSettings BrokerSettings) {
    auto Conf = std::unique_ptr<KafkaConf>(
        KafkaConf::create(RdKafka::Conf::CONF_GLOBAL));
    std::string ErrorStr;
    auto iter = BrokerSettings.KafkaConfiguration.begin();
    while (iter != BrokerSettings.KafkaConfiguration.end()) {
      if (Conf->set(iter->first, iter->second, ErrorStr) !=
          RdKafka::Conf::CONF_OK) {
        throw MetadataException(fmt::format(
            R"(Got error when configuring metadata brokers "{}" setting: "{}")",
            iter->first, ErrorStr));
      }
      ++iter;
    }
    if (Conf->set("metadata.broker.list", Broker, ErrorStr) !=
        RdKafka::Conf::CONF_OK) {
      throw MetadataException(fmt::format(
          R"(Got error when configuring metadata brokers: "{}")", ErrorStr));
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
  getOffsetForTimeImpl(std::string const &Broker, std::string const &Topic,
                       std::vector<int> const &Partitions, time_point Time,
                       duration TimeOut, BrokerSettings BrokerSettings) {
    auto Handle = MetadataEnquirer().getKafkaHandle<KafkaHandle, RdKafka::Conf>(
        Broker, BrokerSettings);
    auto UsedTime = toMilliSeconds(Time);
    std::vector<std::unique_ptr<RdKafka::TopicPartition>> TopicPartitions;
    std::vector<RdKafka::TopicPartition *> TopicPartitionsRaw;
    for (const auto &PartitionId : Partitions) {
      auto CTopicPartition = std::unique_ptr<RdKafka::TopicPartition>(
          RdKafka::TopicPartition::create(Topic, PartitionId, UsedTime));
      TopicPartitionsRaw.emplace_back(CTopicPartition.get());
      TopicPartitions.push_back(std::move(CTopicPartition));
    }

    auto TimeOutInMs = toMilliSeconds(TimeOut);

    auto ReturnCode = Handle->offsetsForTimes(TopicPartitionsRaw, TimeOutInMs);
    if (ReturnCode != RdKafka::ERR_NO_ERROR) {
      throw MetadataException("Failed to query broker for offset corresponding "
                              "to timestamp. Error code was: " +
                              std::to_string(ReturnCode));
    }
    std::vector<std::pair<int, int64_t>> ReturnSet;
    for (const auto &CTopicPartition : TopicPartitions) {
      if (CTopicPartition->err() != RdKafka::ERR_NO_ERROR) {
        throw MetadataException(
            "Error for partition " +
            std::to_string(CTopicPartition->partition()) +
            " when retrieving offset for timestamp. Error code was: " +
            std::to_string(CTopicPartition->err()));
      }
      ReturnSet.emplace_back(std::make_pair(CTopicPartition->partition(),
                                            CTopicPartition->offset()));
    }
    return ReturnSet;
  }

  template <typename MetaDataType>
  std::vector<int> extractPartitionIDs(MetaDataType TopicMetaData) {
    std::vector<int> ReturnVector;
    for (auto const &Partition : *TopicMetaData->partitions()) {
      ReturnVector.push_back(Partition->id());
    }
    return ReturnVector;
  }

  template <class KafkaHandle, class KafkaTopic>
  std::vector<int>
  getPartitionsForTopicImpl(std::string const &Broker, std::string const &Topic,
                            duration TimeOut, BrokerSettings BrokerSettings) {
    auto Handle = MetadataEnquirer().getKafkaHandle<KafkaHandle, RdKafka::Conf>(
        Broker, BrokerSettings);
    std::string ErrorStr;
    auto TopicObj = std::unique_ptr<RdKafka::Topic>(
        KafkaTopic::create(Handle.get(), Topic, nullptr, ErrorStr));
    auto TimeOutInMs = toMilliSeconds(TimeOut);
    RdKafka::Metadata *MetadataPtr{nullptr};
    auto ReturnCode =
        Handle->metadata(true, TopicObj.get(), &MetadataPtr, TimeOutInMs);
    if (ReturnCode != RdKafka::ERR_NO_ERROR) {
      throw MetadataException(fmt::format(
          "Failed to query broker {} for available partitions on topic "
          "{}. Error was: {}",
          Broker, Topic, RdKafka::err2str(ReturnCode)));
    }
    auto TopicMetaData = MetadataEnquirer().findKafkaTopic(Topic, MetadataPtr);
    auto ReturnVector = MetadataEnquirer().extractPartitionIDs(TopicMetaData);
    delete MetadataPtr;
    return ReturnVector;
  }

  std::set<std::string> getTopicList(std::string const &Broker,
                                     duration TimeOut,
                                     BrokerSettings BrokerSettings) {
    auto Handle =
        MetadataEnquirer().getKafkaHandle<RdKafka::Consumer, RdKafka::Conf>(
            Broker, BrokerSettings);
    std::string ErrorStr;
    auto TimeOutInMs = toMilliSeconds(TimeOut);
    RdKafka::Metadata *MetadataPtr{nullptr};
    auto ReturnCode =
        Handle->metadata(true, nullptr, &MetadataPtr, TimeOutInMs);
    if (ReturnCode != RdKafka::ERR_NO_ERROR) {
      throw MetadataException(fmt::format(
          "Failed to query broker for available partitions. Error code was: {}",
          ReturnCode));
    }
    auto Topics = MetadataPtr->topics();
    std::set<std::string> TopicNames;
    for (auto const &CTopic : *Topics) {
      TopicNames.emplace(CTopic->topic());
    }
    delete MetadataPtr;
    return TopicNames;
  }
};

const RdKafka::TopicMetadata *
findTopicMetadata(const std::string &Topic,
                  const RdKafka::Metadata *KafkaMetadata);

std::vector<std::pair<int, int64_t>>
getOffsetForTime(std::string const &Broker, std::string const &Topic,
                 std::vector<int> const &Partitions, time_point Time,
                 duration TimeOut, BrokerSettings BrokerSettings);

std::vector<int> getPartitionsForTopic(std::string const &Broker,
                                       std::string const &Topic,
                                       duration TimeOut,
                                       BrokerSettings BrokerSettings);
} // namespace Kafka
