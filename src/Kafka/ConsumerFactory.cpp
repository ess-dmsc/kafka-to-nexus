// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ConsumerFactory.h"
#include "helper.h"

namespace Kafka {

std::unique_ptr<Consumer> createConsumer(BrokerSettings const &Settings) {
  auto SettingsCopy = Settings;

  // Create a unique group.id for this consumer
  if (SettingsCopy.KafkaConfiguration.find("group.id") ==
      SettingsCopy.KafkaConfiguration.end()) {
    auto GroupIdStr =
        fmt::format("filewriter--streamer--host:{}--pid:{}--time:{}",
                    getHostName(), getPID(),
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count());
    SettingsCopy.KafkaConfiguration.emplace("group.id", GroupIdStr);
  }

  auto Conf = std::unique_ptr<RdKafka::Conf>(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  auto EventCallback = std::make_unique<KafkaEventCb>();
  std::string ErrorString;
  Conf->set("event_cb", EventCallback.get(), ErrorString);
  Conf->set("metadata.broker.list", SettingsCopy.Address, ErrorString);
  configureKafka(*Conf, SettingsCopy);
  auto KafkaConsumer = std::unique_ptr<RdKafka::KafkaConsumer>(
      RdKafka::KafkaConsumer::create(Conf.get(), ErrorString));
  if (KafkaConsumer == nullptr) {
    Logger::Critical("Cannot create kafka consumer: {}", ErrorString);
    throw std::runtime_error("cannot create Kafka consumer");
  }
  return std::make_unique<Consumer>(std::move(KafkaConsumer), std::move(Conf),
                                    std::move(EventCallback));
}

std::shared_ptr<ConsumerInterface>
ConsumerFactory::createConsumer(const BrokerSettings &Settings) {
  return Kafka::createConsumer(Settings);
}

std::shared_ptr<Kafka::ConsumerInterface>
ConsumerFactory::createConsumerAtOffset(Kafka::BrokerSettings const &settings,
                                        std::string const &topic,
                                        int partition_id, int64_t offset) {
  auto consumer = createConsumer(settings);
  consumer->addPartitionAtOffset(topic, partition_id, offset);
  return consumer;
}
} // namespace Kafka
