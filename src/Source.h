#pragma once

#include "FlatbufferReader.h"
#include "HDFFile.h"
#include "HDFWriterModule.h"
#include "Msg.h"
#include "ProcessMessageResult.h"
#include "json.h"
#include <string>

namespace FileWriter {

/// \brief Represents a sourcename on a topic.
///
/// The sourcename can be empty. This is meant for highest efficiency on topics
/// which are exclusively used for only one sourcename.
class Source final {
public:
  Source(std::string Name, std::string ID, HDFWriterModule::ptr Writer);
  Source(Source &&) = default;
  ~Source();
  std::string const &topic() const;
  std::string const &sourcename() const;
  ProcessMessageResult process_message(FlatbufferMessage const &Message);
  void close_writer_module();
  bool is_parallel = false;
  HDFFile *HDFFileForSWMR = nullptr;
  void setTopic(std::string const &Name);

private:
  std::string TopicName;
  std::string SourceName;
  std::string SchemaID;
  std::unique_ptr<HDFWriterModule> WriterModule;
  uint64_t _processed_messages_count = 0;
  uint64_t _cnt_msg_written = 0;
  std::shared_ptr<spdlog::logger> Logger = spdlog::get("filewriterlogger");
};

} // namespace FileWriter
