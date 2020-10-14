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
  FeedbackProducer(std::string const &ServiceIdentifier, uri::URI ResponseUri,
                   Kafka::BrokerSettings Settings);
  FeedbackProducer(std::string const &ServiceIdentifier,
                   std::unique_ptr<Kafka::ProducerTopic> KafkaProducer);
  void publishResponse(ActionResponse Command, ActionResult Result,
                       std::string JobId, std::string CommandId,
                       std::string Description) override;
  void publishStoppedMsg(ActionResult Result, std::string JobId,
                         std::string Description, std::string FileName,
                         std::string Metadata) override;

private:
  std::string ServiceId;
  std::unique_ptr<Kafka::ProducerTopic> Producer;
};

} // namespace Command
