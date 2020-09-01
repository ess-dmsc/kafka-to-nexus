// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ResponseProducer.h"
#include "Kafka/ProducerTopic.h"
#include <answ_action_response_generated.h>

namespace Command {

Kafka::BrokerSettings setBrokerAddress(Kafka::BrokerSettings Settings, std::string NewAddress) {
  Settings.Address = NewAddress;
  return Settings;
}

ResponseProducer::ResponseProducer(const std::string &ServiceIdentifier, std::unique_ptr<Kafka::ProducerTopic> KafkaProducer) : ServiceId(ServiceIdentifier), Producer(std::move(KafkaProducer)) {
}

ResponseProducer::ResponseProducer(const std::string &ServiceIdentifier, uri::URI ResponseUri, Kafka::BrokerSettings Settings) : ResponseProducer(ServiceIdentifier, std::make_unique<Kafka::ProducerTopic>(
      std::make_shared<Kafka::Producer>(setBrokerAddress(Settings, ResponseUri.HostPort)), ResponseUri.Topic)) {
}

void ResponseProducer::publishResponse(ActionResponse Command,
                                       ActionResult Result, std::string JobId, std::string Description) {
  std::map<ActionResponse, ActionType> ActionMap{{ActionResponse::StartJob, ActionType::StartJob}, {ActionResponse::SetStopTime, ActionType::SetStopTime}, {ActionResponse::StopNow, ActionType::StopNow}, {ActionResponse::HasStopped, ActionType::HasStopped}};
  std::map<ActionResult, ActionOutcome> OutcomeMap{{ActionResult::Success, ActionOutcome::Success}, {ActionResult::Failure, ActionOutcome::Failure}};
  flatbuffers::FlatBufferBuilder Builder;
  auto ServiceIdStr = Builder.CreateString(ServiceId);
  auto JobIdStr = Builder.CreateString(JobId);
  auto ErrorMsgString = Builder.CreateString(Description);
  auto ResponseFlatbuffer = CreateActionResponse(Builder, ServiceIdStr, JobIdStr, ActionMap[Command], OutcomeMap[Result], ErrorMsgString);
  FinishActionResponseBuffer(Builder, ResponseFlatbuffer);
  Producer->produce(Builder.Release());
}

} // namespace Command