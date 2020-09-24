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
#include "Kafka/Producer.h"
#include "Kafka/ProducerTopic.h"
#include "URI.h"
#include "ResponseProducerBase.h"
#include <string>

namespace Command {

class ResponseProducer : public ResponseProducerBase {
public:
  ResponseProducer(std::string const &ServiceIdentifier, uri::URI ResponseUri,
                   Kafka::BrokerSettings Settings);
  ResponseProducer(std::string const &ServiceIdentifier,
                   std::unique_ptr<Kafka::ProducerTopic> KafkaProducer);
  void publishResponse(ActionResponse Command, ActionResult Result,
                       std::string JobId, std::string CommandId,
                       std::string Description);

private:
  std::string ServiceId;
  std::unique_ptr<Kafka::ProducerTopic> Producer;
};

} // namespace Command
