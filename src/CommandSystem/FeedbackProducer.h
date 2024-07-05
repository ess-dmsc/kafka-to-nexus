// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "FeedbackProducerBase.h"
#include "Kafka/BrokerSettings.h"
#include "Kafka/Producer.h"
#include "Kafka/ProducerTopic.h"
#include "URI.h"
#include <string>

namespace Command {

class FeedbackProducer : public FeedbackProducerBase {
public:
  std::unique_ptr<FeedbackProducer> static create(
      std::string const &ServiceIdentifier, uri::URI const &ResponseUri,
      Kafka::BrokerSettings Settings);
  std::unique_ptr<FeedbackProducer> static create_null(
      std::string const &ServiceIdentifier,
      std::unique_ptr<Kafka::StubProducerTopic> producer);
  FeedbackProducer(std::string ServiceIdentifier,
                   std::unique_ptr<Kafka::IProducerTopic> KafkaProducer);
  void publishResponse(ActionResponse Command, ActionResult Result,
                       std::string const &JobId, std::string const &CommandId,
                       time_point StopTime, int StatusCode,
                       std::string const &Description) override;
  void publishStoppedMsg(ActionResult Result, std::string const &JobId,
                         std::string const &Description,
                         std::filesystem::path FilePath,
                         std::string const &Metadata) override;

private:
  std::string ServiceId;
  std::unique_ptr<Kafka::IProducerTopic> Producer;
};

} // namespace Command
