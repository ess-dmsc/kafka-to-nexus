// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

namespace Command {

enum class ActionResponse { StartJob, SetStopTime, StopNow, HasStopped};

enum class ActionResult {
  Success,
  Failure,
};

class ResponseProducerBase {
public:
  ResponseProducerBase() = default;
  virtual ~ResponseProducerBase() = default;

  virtual void publishResponse(ActionResponse Command, ActionResult Result,
                       std::string JobId, std::string CommandId,
                       std::string Description) = 0;
};

} // namespace Command
