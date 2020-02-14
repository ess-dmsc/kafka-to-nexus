// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <6s4t_run_stop_generated.h>
#include <pl72_run_start_generated.h>

flatbuffers::DetachedBuffer buildRunStartMessage(
    std::string const &InstrumentName, std::string const &RunName,
    std::string const &NexusStructure, std::string const &JobID,
    std::string const &ServiceID, std::string const &Broker,
    std::string const &Filename, uint64_t StartTime, uint64_t StopTime) {
  flatbuffers::FlatBufferBuilder Builder;

  const auto InstrumentNameOffset = Builder.CreateString(InstrumentName);
  const auto RunIDOffset = Builder.CreateString(RunName);
  const auto NexusStructureOffset = Builder.CreateString(NexusStructure);
  const auto JobIDOffset = Builder.CreateString(JobID);
  const auto ServiceIDOffset = Builder.CreateString(ServiceID);
  const auto BrokerOffset = Builder.CreateString(Broker);
  const auto FilenameOffset = Builder.CreateString(Filename);

  auto messageRunStart =
      CreateRunStart(Builder, StartTime, StopTime, RunIDOffset,
                     InstrumentNameOffset, NexusStructureOffset, JobIDOffset,
                     BrokerOffset, ServiceIDOffset, FilenameOffset);

  FinishRunStartBuffer(Builder, messageRunStart);
  return Builder.Release();
}

flatbuffers::DetachedBuffer buildRunStopMessage(uint64_t StopTime,
                                                std::string const &RunName,
                                                std::string const &JobID,
                                                std::string const &ServiceID) {
  flatbuffers::FlatBufferBuilder Builder;

  const auto RunIDOffset = Builder.CreateString(RunName);
  const auto JobIDOffset = Builder.CreateString(JobID);
  const auto ServiceIDOffset = Builder.CreateString(ServiceID);

  auto messageRunStop = CreateRunStop(Builder, StopTime, RunIDOffset,
                                      JobIDOffset, ServiceIDOffset);

  FinishRunStopBuffer(Builder, messageRunStop);
  return Builder.Release();
}

std::string const InstrumentNameInput = "TEST";
std::string const RunNameInput = "42";
std::string const NexusStructureInput = "{}";
std::string const JobIDInput = "qw3rty";
std::string const ServiceIDInput = "filewriter1";
std::string const BrokerInput = "somehost:1234";
std::string const FilenameInput = "a-dummy-name-01.h5";
uint64_t const StartTimeInput = 123456789000;
uint64_t const StopTimeInput = 123456790000;
