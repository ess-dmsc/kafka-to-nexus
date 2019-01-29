#include "ConsumerFactory.h"

namespace KafkaW {

std::unique_ptr<Consumer> createConsumer(const BrokerSettings &Settings) {
  std::string ErrorString;
  auto Conf = std::unique_ptr<RdKafka::Conf>(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  auto EventCallback = std::make_unique<KafkaEventCb>();
  Conf->set("event_cb", EventCallback.get(), ErrorString);
  Conf->set("metadata.broker.list", Settings.Address, ErrorString);
  Settings.apply(Conf.get());
  auto KafkaConsumer = std::unique_ptr<RdKafka::KafkaConsumer>(
      RdKafka::KafkaConsumer::create(Conf.get(), ErrorString));
  if (KafkaConsumer == nullptr) {
    LOG(Sev::Error, "can not create kafka consumer: {}", ErrorString);
    throw std::runtime_error("can not create Kafka consumer");
  }
  return std::make_unique<Consumer>(std::move(KafkaConsumer), std::move(Conf),
                                    std::move(EventCallback));
}
} // namespace KafkaW
