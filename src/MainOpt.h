// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "StreamerOptions.h"
#include "URI.h"
#include "helper.h"
#include "json.h"
#include "logger.h"

#include <chrono>
#include <string>
#include <vector>

// POD
struct MainOpt {
  static std::string getDefaultServiceId();
  /// \brief Each running filewriter is identifiable by an id.
  ///
  /// This `service_id` is announced in the status updates.
  /// It is by default a combination of hostname and process id.
  /// Can be set via command line or configuration file.
  std::string ServiceName;
  void setServiceName(std::string NewServiceName);

  [[nodiscard]] std::string getServiceId() const;

  [[nodiscard]] std::string getHDFOutputPrefix() const;

  [[nodiscard]] std::string getHDFTemplatePrefix() const;

  /// \brief Streamer options are parsed from the configuration file and passed
  /// on to the StreamController.
  FileWriter::StreamerOptions StreamerConfiguration;

  /// Command line argument to print application version and exit.
  bool PrintVersion = false;

  LogSeverity LoggingLevel{LogSeverity::Info};

  uri::URI GrafanaCarbonAddress;

  /// Kafka topic where file writer commands are published.
  std::string command_topic;

  /// Kafka topic where file writer jobs are published.
  std::string job_pool_topic;

  /// \brief Path for HDF output.
  ///
  /// This gets prepended to the HDF output filename given in the write
  /// commands.
  std::string HDFOutputPrefix{std::filesystem::current_path()};

  /// \brief Path for HDF template.
  ///
  /// This gets prepended to the HDF template filename given in the write
  /// commands.
  std::string HDFTemplatePrefix{std::filesystem::current_path()};

  /// Used for command line argument.
  bool ListWriterModules = false;

  /// \brief Interval to publish status of `Master`
  /// (e.g. list of current file writings).
  duration StatusMasterInterval{2000ms};

  std::vector<std::string> brokers;

private:
  std::string ServiceId{getDefaultServiceId()};
};

void setupLoggerFromOptions(MainOpt const &opt);
