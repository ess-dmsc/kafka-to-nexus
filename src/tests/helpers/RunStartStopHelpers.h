// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

namespace FileWriter {
struct Msg;
}

namespace RunStartStopHelpers {
FileWriter::Msg buildRunStartMessage(
    std::string const &InstrumentName, std::string const &RunName,
    std::string const &NexusStructure, std::string const &JobID,
    std::string const &ServiceID, std::string const &Broker,
    std::string const &Filename, uint64_t StartTime, uint64_t StopTime);

FileWriter::Msg buildRunStopMessage(uint64_t StopTime,
                                    std::string const &RunName,
                                    std::string const &JobID,
                                    std::string const &ServiceID);
} // namespace RunStartStopHelpers
