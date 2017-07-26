#include "logger.h"
#include "KafkaW.h"
#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <string>
#include <thread>
#ifdef HAVE_GRAYLOG_LOGGER
#include <graylog_logger/GraylogInterface.hpp>
#include <graylog_logger/Log.hpp>
#endif

int log_level = 3;

// adhoc namespace because it would now collide with ::Logger defined
// in gralog_logger

namespace DW {

class Logger {
public:
  Logger();
  ~Logger();
  void use_log_file(std::string fname);
  void log_kafka_gelf_start(std::string broker, std::string topic);
  void log_kafka_gelf_stop();
  FILE *log_file = stdout;
  void dwlog_inner(int level, char const *file, int line, char const *func,
                   std::string const &s1);
  int prefix_len();
  void fwd_graylog_logger_enable(std::string address);

private:
  std::atomic<bool> do_run_kafka{false};
  std::atomic<bool> do_use_graylog_logger{false};
  std::shared_ptr<KafkaW::Producer> producer;
  std::unique_ptr<KafkaW::Producer::Topic> topic;
  std::thread thread_poll;
};

Logger::Logger() {}

Logger::~Logger() {
  do_run_kafka = false;
  if (log_file != nullptr and log_file != stdout) {
    LOG(0, "Closing log");
    fclose(log_file);
  }
  if (thread_poll.joinable()) {
    thread_poll.join();
  }
}

void Logger::use_log_file(std::string fname) {
  FILE *f1 = fopen(fname.c_str(), "wb");
  log_file = f1;
}

void Logger::log_kafka_gelf_start(std::string address, std::string topicname) {
  KafkaW::BrokerOpt opt;
  opt.address = address;
  producer.reset(new KafkaW::Producer(opt));
  topic.reset(new KafkaW::Producer::Topic(producer, topicname));
  topic->do_copy();
  thread_poll = std::thread([this] {
    while (do_run_kafka.load()) {
      producer->poll();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  });
  do_run_kafka = true;
}

void Logger::log_kafka_gelf_stop() {
  do_run_kafka = false;
  // Wait a bit with the cleanup...
  // auto t = topic.exchange(nullptr);
  // auto p = producer.exchange(nullptr);
}

void Logger::fwd_graylog_logger_enable(std::string address) {
  auto addr = address;
  int port = 12201;
  auto col = address.find(":");
  if (col != std::string::npos) {
    addr = address.substr(0, col);
    port = strtol(address.c_str() + col + 1, nullptr, 10);
  }
#ifdef HAVE_GRAYLOG_LOGGER
  Log::RemoveAllHandlers();
  LOG(4, "Enable graylog_logger on {}:{}", addr, port);
  Log::AddLogHandler(new GraylogInterface(addr, port));
  do_use_graylog_logger = true;
#else
  LOG(0, "ERROR not compiled with support for graylog_logger. Would have used "
         "{}:{}",
      addr, port);
#endif
}

void Logger::dwlog_inner(int level, char const *file, int line,
                         char const *func, std::string const &s1) {
  int npre = prefix_len();
  int const n2 = strlen(file);
  if (npre > n2) {
    // fmt::print(log_file, "ERROR in logging API: npre > n2\n");
    npre = 0;
  }
  auto f1 = file + npre;
  auto lmsg = fmt::format("{}:{} [{}]:  {}\n", f1, line, level, s1);
  fwrite(lmsg.c_str(), 1, lmsg.size(), log_file);
  if (level < 7 && do_run_kafka.load()) {
    // If we will use logging to Kafka in the future, refactor a bit to reduce
    // duplicate work..
    using namespace rapidjson;
    Document d;
    auto &a = d.GetAllocator();
    d.SetObject();
    d.AddMember("version", "1.1", a);
    d.AddMember("short_message", Value(lmsg.c_str(), a), a);
    d.AddMember("level", Value(level), a);
    d.AddMember("_FILE", Value(file, a), a);
    d.AddMember("_LINE", Value(line), a);
    StringBuffer buf1;
    Writer<StringBuffer> wr(buf1);
    d.Accept(wr);
    auto s1 = buf1.GetString();
    topic->produce((unsigned char *)s1, strlen(s1));
  }
#ifdef HAVE_GRAYLOG_LOGGER
  if (do_use_graylog_logger.load() and level < 7) {
    Log::Msg(level, lmsg);
  }
#endif
  // fflush(log_file);
}

int Logger::prefix_len() {
  static int n1 = strlen(__FILE__) - 10;
  return n1;
}

static Logger g__logger;

} // namespace DW

void use_log_file(std::string fname) { DW::g__logger.use_log_file(fname); }

void dwlog_inner(int level, char const *file, int line, char const *func,
                 std::string const &s1) {
  DW::g__logger.dwlog_inner(level, file, line, func, s1);
}

void log_kafka_gelf_start(std::string broker, std::string topic) {
  DW::g__logger.log_kafka_gelf_start(broker, topic);
}

void log_kafka_gelf_stop() {}

void fwd_graylog_logger_enable(std::string address) {
  DW::g__logger.fwd_graylog_logger_enable(address);
}
