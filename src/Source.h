#pragma once

#include "FlatbufferReader.h"
#include "HDFFile.h"
#include "HDFWriterModule.h"
#include "MessageTimestamp.h"
#include "Msg.h"
#include "ProcessMessageResult.h"
#include "json.h"
#include <string>

class Test___FileWriterTask___Create01;
class CommandHandler_Test;

namespace FileWriter {

class Result {
public:
  static Result Ok();
  bool is_OK();
  bool is_ERR();

private:
  int _res = 0;
};

/// \brief Represents a sourcename on a topic.
///
/// The sourcename can be empty.
/// This is meant for highest efficiency on topics which are exclusively used
/// for only one sourcename.
class Source final {
public:
  Source(std::string const &Name, std::string const &ID,
         HDFWriterModule::ptr Writer);
  Source(Source &&) noexcept;
  ~Source();
  std::string const &topic() const;
  std::string const &sourcename() const;
  uint64_t processed_messages_count() const;
  ProcessMessageResult process_message(FlatbufferMessage const &Message);
  std::string to_str() const;
  nlohmann::json to_json() const;
  void close_writer_module();
  bool is_parallel = false;
  HDFFile *HDFFileForSWMR = nullptr;

private:
  std::string _topic;
  std::string SourceName;
  std::string SchemaID;
  std::unique_ptr<HDFWriterModule> WriterModule;

  uint64_t _processed_messages_count = 0;
  uint64_t _cnt_msg_written = 0;

  bool do_process_message = true;

  friend class CommandHandler;
  friend class FileWriterTask;
  friend class SourceFactory_by_FileWriterTask;
  friend class ::Test___FileWriterTask___Create01;
  friend void swap(Source &x, Source &y);
};

} // namespace FileWriter
