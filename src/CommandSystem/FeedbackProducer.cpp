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

#include <utility>

namespace Command {

Kafka::BrokerSettings setBrokerAddress(Kafka::BrokerSettings Settings,
                                       std::string NewAddress) {
  Settings.Address = std::move(NewAddress);
  return Settings;
}

FeedbackProducer::FeedbackProducer(
    std::string ServiceIdentifier,
    std::unique_ptr<Kafka::IProducerTopic> KafkaProducer)
    : ServiceId(std::move(ServiceIdentifier)),
      Producer(std::move(KafkaProducer)) {}

std::unique_ptr<FeedbackProducer>
FeedbackProducer::create(const std::string &service_identifier,
                         uri::URI const &response_uri,
                         Kafka::BrokerSettings settings) {
  return std::make_unique<FeedbackProducer>(
      service_identifier,
      std::make_unique<Kafka::ProducerTopic>(
          std::make_shared<Kafka::Producer>(
              setBrokerAddress(std::move(settings), response_uri.HostPort)),
          response_uri.Topic));
}

std::unique_ptr<FeedbackProducer> FeedbackProducer::create_null(
    const std::string &service_identifier,
    std::unique_ptr<Kafka::StubProducerTopic> producer) {
  return std::make_unique<FeedbackProducer>(service_identifier,
                                            std::move(producer));
}

void FeedbackProducer::publishResponse(ActionResponse Command,
                                       ActionResult Result,
                                       std::string const &JobId,
                                       std::string const &CommandId,
                                       time_point StopTime, int StatusCode,
                                       std::string const &Description) {
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
      StatusCode, toMilliSeconds(StopTime), ErrorMsgString, CommandIdString);
  FinishActionResponseBuffer(Builder, ResponseFlatbuffer);
  Producer->produce(Builder.Release());
}

void FeedbackProducer::publishStoppedMsg(ActionResult Result,
                                         std::string const &JobId,
                                         std::string const &Description,
                                         std::filesystem::path FilePath,
                                         std::string const &Metadata) {
  flatbuffers::FlatBufferBuilder Builder;
  std::map<ActionResult, bool> OutcomeMap{{ActionResult::Success, false},
                                          {ActionResult::Failure, true}};
  auto ServiceIdStr = Builder.CreateString(ServiceId);
  auto JobIdStr = Builder.CreateString(JobId);
  auto FilePathStr = Builder.CreateString(FilePath.string());
  auto MetadataStr = Builder.CreateString(Metadata);
  auto ErrorMsgString = Builder.CreateString(Description);
  auto StoppedFlatbuffer =
      CreateFinishedWriting(Builder, ServiceIdStr, JobIdStr, OutcomeMap[Result],
                            FilePathStr, MetadataStr, ErrorMsgString);
  FinishFinishedWritingBuffer(Builder, StoppedFlatbuffer);
  Producer->produce(Builder.Release());
}

} // namespace Command
