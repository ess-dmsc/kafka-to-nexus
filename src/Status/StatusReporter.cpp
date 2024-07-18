// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "StatusReporter.h"
#include "json.h"

namespace Status {

void StatusReporter::start() {
  Logger::Trace("Starting the StatusTimer");
  AsioTimer.async_wait([this](std::error_code const &Error) {
    if (Error != asio::error::operation_aborted) {
      this->reportStatus();
    }
  });
  StatusThread = std::thread(&StatusReporter::run, this);
}

void StatusReporter::waitForStop() {
  Logger::Trace("Stopping StatusTimer");
  IO.stop();
  StatusThread.join();
}

void StatusReporter::postReportStatusActions() {
  AsioTimer.expires_at(AsioTimer.expires_at() + Period);
  AsioTimer.async_wait([this](std::error_code const &Error) {
    if (Error != asio::error::operation_aborted) {
      this->reportStatus();
    }
  });
}

StatusReporter::~StatusReporter() { this->waitForStop(); }

} // namespace Status
