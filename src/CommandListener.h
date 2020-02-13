// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "KafkaW/Consumer.h"
#include "MainOpt.h"
#include "logger.h"

namespace FileWriter {

/// Check for new commands on the topic, return them to the Master.
class CommandListener {
public:
  explicit CommandListener(MainOpt &Config);
  virtual ~CommandListener() = default;

  /// Start listening to command messages.
  virtual void start();

  /// Check for new command packets and return one if there is.
  virtual std::pair<KafkaW::PollStatus, Msg> poll();

private:
  MainOpt &config;
  std::unique_ptr<KafkaW::ConsumerInterface> consumer;
  SharedLogger Logger = getLogger();
};
} // namespace FileWriter
