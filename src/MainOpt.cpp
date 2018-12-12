#include "MainOpt.h"
#include "URI.h"
#include "helper.h"
#include "json.h"
#include <iostream>

using uri::URI;

// For reasons unknown, the presence of the constructor caused the integration
// test to fail, with the NeXus file being created, but no data written to it.
// While the cause of this problem is not discovered and fixed, use the
// following init function.
void MainOpt::init() {
  service_id = fmt::format("kafka-to-nexus--host:{}--pid:{}",
                           gethostname_wrapper(), getpid_wrapper());
}

int MainOpt::parseJsonCommands() {
  if (CommandsJsonFilename.empty()) {
    LOG(Sev::Notice, "given config filename is empty");
    return -1;
  }
  auto jsontxt = gulp(CommandsJsonFilename);
  using nlohmann::json;
  try {
    CommandsJson = json::parse(jsontxt);
  } catch (...) {
    return 1;
  }
  findAndAddCommands();
  return 0;
}

void MainOpt::findAndAddCommands() {
  if (auto v = find<nlohmann::json>("commands", CommandsJson)) {
    for (auto const &Command : v.inner()) {
      CommandsFromJson.emplace_back(Command.dump());
    }
  }
}

void setupLoggerFromOptions(MainOpt const &opt) {
  g_ServiceID = opt.service_id;
  if (!opt.kafka_gelf.empty()) {
    URI uri(opt.kafka_gelf);
    log_kafka_gelf_start(uri.HostPort, uri.Topic);
    LOG(Sev::Debug, "Enabled kafka_gelf: //{}/{}", uri.HostPort, uri.Topic);
  }

  if (!opt.graylog_logger_address.empty()) {
    fwd_graylog_logger_enable(opt.graylog_logger_address);
  }

  if (!opt.LogFilename.empty()) {
    use_log_file(opt.LogFilename);
  }
}
