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
  /// If true the filewriter aborts the whole job if one or more streams are
  /// misconfigured and fail to start
  bool AbortOnUninitialisedStream = false;

  /// \brief Each running filewriter is identifiable by an id.
  ///
  /// This `service_id` is announced in the status updates.
  /// It is by default a combination of hostname and process id.
  /// Can be set via command line or configuration file.
  std::string ServiceName;
  void setServiceName(std::string NewServiceName);

  std::string getServiceId() const;

  /// \brief Streamer options are parsed from the configuration file and passed
  /// on to the StreamController.
  FileWriter::StreamerOptions StreamerConfiguration;

  /// Command line argument to print application version and exit.
  bool PrintVersion = false;

  spdlog::level::level_enum LoggingLevel;

  /// Can optionally use the `graylog_logger` library to log to this address.
  uri::URI GraylogLoggerAddress;

  uri::URI GrafanaCarbonAddress;

  /// Used for logging to file
  std::string LogFilename;

  /// Kafka broker and topic where file writer commands are published.
  uri::URI CommandBrokerURI{"localhost:9092/kafka-to-nexus.command"};

  /// Kafka broker and topic where file writer jobs are published.
  uri::URI JobPoolURI{"localhost:9092/kafka-to-nexus.job_pool"};

  /// \brief Path for HDF output.
  ///
  /// This gets prepended to the HDF output filename given in the write
  /// commands.
  std::string HDFOutputPrefix;

  /// Used for command line argument.
  bool ListWriterModules = false;

  /// \brief Interval to publish status of `Master`
  /// (e.g. list of current file writings).
  std::chrono::milliseconds StatusMasterIntervalMS{2000};

private:
  std::string ServiceId{getDefaultServiceId()};
};

void setupLoggerFromOptions(MainOpt const &opt);
