#include "ConsumerFactory.h"
#include "helper.h"

namespace KafkaW {

std::unique_ptr<Consumer> createConsumer(const BrokerSettings &Settings,
                                         const std::string &Broker) {
  auto SettingsCopy = Settings;

  // Create a unique group.id for this consumer
  SettingsCopy.KafkaConfiguration["group.id"] = fmt::format(
      "filewriter--streamer--host:{}--pid:{}--time:{}", gethostname_wrapper(),
      getpid_wrapper(), std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count());
  SettingsCopy.Address = Broker;

  auto Conf = std::unique_ptr<RdKafka::Conf>(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  auto EventCallback = std::make_unique<KafkaEventCb>();
  std::string ErrorString;
  Conf->set("event_cb", EventCallback.get(), ErrorString);
  Conf->set("metadata.broker.list", SettingsCopy.Address, ErrorString);
    configureKafka(Conf.get(), SettingsCopy);
  auto KafkaConsumer = std::unique_ptr<RdKafka::KafkaConsumer>(
      RdKafka::KafkaConsumer::create(Conf.get(), ErrorString));
  if (KafkaConsumer == nullptr) {
    spdlog::get("filewriterlogger")
        ->error("can not create kafka consumer: {}", ErrorString);
    throw std::runtime_error("can not create Kafka consumer");
  }
  return std::make_unique<Consumer>(std::move(KafkaConsumer), std::move(Conf),
                                    std::move(EventCallback));
}
} // namespace KafkaW
