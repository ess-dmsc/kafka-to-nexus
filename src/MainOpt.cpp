// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "MainOpt.h"
#include <exception>

constexpr size_t RandomStringLength{4};

void setupLoggerFromOptions(MainOpt const &opt) {
  setUpLogging(opt.LoggingLevel, opt.LogFilename, opt.GraylogLoggerAddress);
}

std::string MainOpt::getDefaultServiceId() {
  return fmt::format("kafka-to-nexus:{}-pid:{}-{}", getHostName(), getPID(),
                     randomHexString(RandomStringLength));
}

void MainOpt::setServiceName(std::string NewServiceName) {
  ServiceName = std::move(NewServiceName);
  if (ServiceName.empty()) {
    ServiceId = getDefaultServiceId();
  } else {
    ServiceId = fmt::format("{}-pid:{}-{}", ServiceName, getPID(),
                            randomHexString(RandomStringLength));
  }
}

std::string MainOpt::getServiceId() const {
  if (ServiceId.empty()) {
    throw std::runtime_error("Service id is empty.");
  }
  return ServiceId;
}

std::string MainOpt::getHDFOutputPrefix() const {
  return HDFOutputPrefix;
}
