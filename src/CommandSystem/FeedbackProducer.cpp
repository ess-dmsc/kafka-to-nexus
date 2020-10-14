// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FeedbackProducer.h"
#include "Kafka/ProducerTopic.h"
#include <answ_action_response_generated.h>
#include <wrdn_finished_writing_generated.h>

namespace Command {

Kafka::BrokerSettings setBrokerAddress(Kafka::BrokerSettings Settings,
                                       std::string NewAddress) {
  Settings.Address = std::move(NewAddress);
  return Settings;
}

FeedbackProducer::FeedbackProducer(
    const std::string &ServiceIdentifier,
    std::unique_ptr<Kafka::ProducerTopic> KafkaProducer)
    : ServiceId(ServiceIdentifier), Producer(std::move(KafkaProducer)) {}

FeedbackProducer::FeedbackProducer(const std::string &ServiceIdentifier,
                                   uri::URI ResponseUri,
                                   Kafka::BrokerSettings Settings)
    : FeedbackProducer(ServiceIdentifier,
                       std::make_unique<Kafka::ProducerTopic>(
                           std::make_shared<Kafka::Producer>(setBrokerAddress(
                               Settings, ResponseUri.HostPort)),
                           ResponseUri.Topic)) {}

void FeedbackProducer::publishResponse(ActionResponse Command,
                                       ActionResult Result, std::string JobId,
                                       std::string CommandId,
                                       std::string Description) {
  std::map<ActionResponse, ActionType> ActionMap{
      {ActionResponse::StartJob, ActionType::StartJob},
      {ActionResponse::SetStopTime, ActionType::SetStopTime}};
  std::map<ActionResult, ActionOutcome> OutcomeMap{
      {ActionResult::Success, ActionOutcome::Success},
      {ActionResult::Failure, ActionOutcome::Failure}};
  flatbuffers::FlatBufferBuilder Builder;
  auto ServiceIdStr = Builder.CreateString(ServiceId);
  auto JobIdStr = Builder.CreateString(JobId);
  auto ErrorMsgString = Builder.CreateString(Description);
  auto CommandIdString = Builder.CreateString(CommandId);
  auto ResponseFlatbuffer = CreateActionResponse(
      Builder, ServiceIdStr, JobIdStr, ActionMap[Command], OutcomeMap[Result],
      0, 0, ErrorMsgString, CommandIdString);
  FinishActionResponseBuffer(Builder, ResponseFlatbuffer);
  Producer->produce(Builder.Release());
}

void FeedbackProducer::publishStoppedMsg(ActionResult Result, std::string JobId,
                                         std::string Description,
                                         std::string FileName,
                                         std::string Metadata) {
  flatbuffers::FlatBufferBuilder Builder;
  std::map<ActionResult, bool> OutcomeMap{{ActionResult::Success, false},
                                          {ActionResult::Failure, true}};
  auto ServiceIdStr = Builder.CreateString(ServiceId);
  auto JobIdStr = Builder.CreateString(JobId);
  auto FileNameStr = Builder.CreateString(FileName);
  auto MetadataStr = Builder.CreateString(Metadata);
  auto ErrorMsgString = Builder.CreateString(Description);
  auto StoppedFlatbuffer =
      CreateFinishedWriting(Builder, ServiceIdStr, JobIdStr, OutcomeMap[Result],
                            FileNameStr, MetadataStr, ErrorMsgString);
  FinishFinishedWritingBuffer(Builder, StoppedFlatbuffer);
  Producer->produce(Builder.Release());
}

} // namespace Command