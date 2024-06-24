// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "TimeUtility.h"
#include <filesystem>
#include <string>

namespace Command {

enum class ActionResponse { StartJob, SetStopTime };

enum class ActionResult {
  Success,
  Failure,
};

class FeedbackProducerBase {
public:
  FeedbackProducerBase() = default;
  virtual ~FeedbackProducerBase() = default;

  virtual void publishResponse(ActionResponse Command, ActionResult Result,
                               std::string const &JobId,
                               std::string const &CommandId,
                               time_point StopTime, int StatusCode,
                               std::string const &Description) = 0;
  virtual void publishStoppedMsg(ActionResult Result, std::string const &JobId,
                                 std::string const &Description,
                                 std::filesystem::path FilePath,
                                 std::string const &Metadata) = 0;
};

} // namespace Command
