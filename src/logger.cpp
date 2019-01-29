#include "logger.h"
#include "KafkaW/KafkaW.h"
#include "json.h"
#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#ifdef HAVE_GRAYLOG_LOGGER
#include <graylog_logger/GraylogInterface.hpp>
#include <graylog_logger/Log.hpp>
#endif

int log_level = 3;
std::string g_ServiceID;

// adhoc namespace because it would now collide with ::Logger defined
// in gralog_logger

namespace DW {

class Logger {
public:
  Logger();
  ~Logger();
  void use_log_file(std::string fname);
  void log_kafka_gelf_start(std::string const &Address, std::string TopicName);
  FILE *log_file = stdout;
  void dwlog_inner(int level, char const *file, int line, char const *func,
                   std::string const &s1);
  static int prefix_len();
  void fwd_graylog_logger_enable(std::string const &Address);

private:
  std::atomic<bool> do_run_kafka{false};
  std::atomic<bool> do_use_graylog_logger{false};
  std::shared_ptr<KafkaW::Producer> producer;
  std::unique_ptr<KafkaW::ProducerTopic> topic;
  std::thread thread_poll;
};

Logger::Logger() {}

Logger::~Logger() {
  do_run_kafka = false;
  if (log_file != nullptr and log_file != stdout) {
    LOG(Sev::Info, "Closing log");
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

void Logger::log_kafka_gelf_start(std::string const &Address,
                                  std::string TopicName) {
  KafkaW::BrokerSettings BrokerSettings;
  BrokerSettings.Address = Address;
  //  producer.reset(new KafkaW::Producer(BrokerSettings));
  topic.reset(new KafkaW::ProducerTopic(producer, TopicName));
  topic->enableCopy();
  thread_poll = std::thread([this] {
    while (do_run_kafka.load()) {
      producer->poll();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  });
  do_run_kafka = true;
}

void Logger::fwd_graylog_logger_enable(std::string const &Address) {
  auto addr = Address;
  int port = 12201;
  auto col = Address.find(":");
  if (col != std::string::npos) {
    addr = Address.substr(0, col);
    port = strtol(Address.c_str() + col + 1, nullptr, 10);
  }
#ifdef HAVE_GRAYLOG_LOGGER
  Log::RemoveAllHandlers();
  LOG(Sev::Info, "Enable graylog_logger on {}:{}", addr, port);
  Log::AddLogHandler(new GraylogInterface(addr, port));
  do_use_graylog_logger = true;
#else
  LOG(Sev::Notice,
      "ERROR not compiled with support for graylog_logger. Would have used "
      "{}:{}",
      addr, port);
#endif
}

void Logger::dwlog_inner(int level, char const *file, int line,
                         char const *func, std::string const &s1) {
  int npre = Logger::prefix_len();
  int const n2 = strlen(file);
  if (npre > n2) {
    // fmt::print(log_file, "ERROR in logging API: npre > n2\n");
    npre = 0;
  }
  auto f1 = file + npre;
  auto lmsg = fmt::format("{}:{} [{}] [ServiceID:{}]:  {}\n", f1, line, level,
                          g_ServiceID, s1);
  fwrite(lmsg.c_str(), 1, lmsg.size(), log_file);
  if (level < 7 && do_run_kafka.load()) {
    // If we will use logging to Kafka in the future, refactor a bit to reduce
    // duplicate work..
    auto Doc = nlohmann::json::object();
    Doc["version"] = "1.1";
    Doc["short_message"] = lmsg;
    Doc["level"] = level;
    Doc["_FILE"] = file;
    Doc["_LINE"] = line;
    if (!g_ServiceID.empty()) {
      Doc["ServiceID"] = g_ServiceID;
    }
    auto Buffer = Doc.dump();
    topic->produce((unsigned char *)Buffer.data(), Buffer.size());
  }
#ifdef HAVE_GRAYLOG_LOGGER
  if (do_use_graylog_logger.load() and level < 7) {
    Log::Message(level, lmsg);
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

void log_kafka_gelf_start(std::string const &Address, std::string TopicName) {
  DW::g__logger.log_kafka_gelf_start(Address, TopicName);
}

void fwd_graylog_logger_enable(std::string const &Address) {
  DW::g__logger.fwd_graylog_logger_enable(Address);
}
