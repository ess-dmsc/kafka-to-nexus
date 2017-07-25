#include "Master.h"
#include "CommandHandler.h"
#include "FileWriterTask.h"
#include "Source.h"
#include "logger.h"
#include <chrono>
// #include "helper.h"
// #include "utils.h"
#include "commandproducer.h"

namespace FileWriter {

using std::vector;
using std::string;

std::string &CmdMsg_K::str() { return _str; }

Master::Master(MasterConfig &config)
    : config(config), command_listener(config.command_listener) {}

void Master::handle_command_message(std::unique_ptr<KafkaW::Msg> &&msg) {
  CommandHandler command_handler(this, config.config_file);
  command_handler.handle({(char *)msg->data(), (int32_t)msg->size()});
}

void Master::handle_command(rapidjson::Document const &cmd) {
  CommandHandler command_handler(this, config.config_file);
  command_handler.handle(cmd);
}

void Master::run() {
  for (auto const &cmd : config.commands_from_config_file) {
    this->handle_command(cmd);
  }
  command_listener.start();
  while (do_run) {
    LOG(7, "Master poll");
    auto p = command_listener.poll();
    if (auto msg = p.is_Msg()) {
      LOG(7, "Handle a command");
      this->handle_command_message(std::move(msg));
    }
  }
  LOG(6, "calling stop on all stream_masters");
  for (auto &x : stream_masters) {
    x->stop();
  }
  LOG(6, "called stop on all stream_masters");
}

void Master::stop() { do_run = false; }

} // namespace FileWriter
