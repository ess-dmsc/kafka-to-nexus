#pragma once

#include "FlatbufferReader.h"
#include "HDFFile.h"
#include "HDFWriterModule.h"
#include "Msg.h"
#include "ProcessMessageResult.h"
#include "TimeDifferenceFromMessage.h"
#include "json.h"
#include <string>

class Test___FileWriterTask___Create01;

namespace FileWriter {

class Result {
public:
  static Result Ok();
  bool is_OK();
  bool is_ERR();

private:
  int _res = 0;
};

/// Represents a sourcename on a topic.
/// The sourcename can be empty.
/// This is meant for highest efficiency on topics which are exclusively used
/// for only one sourcename.
class Source {
public:
  Source(Source &&) noexcept;
  ~Source() = default;
  std::string const &topic() const;
  std::string const &sourcename() const;
  /// If non-empty, specifies the broker where the data of this Source can be
  /// found.
  std::string const &broker() const;
  uint32_t processed_messages_count() const;
  ProcessMessageResult process_message(Msg const &msg);
  std::string to_str() const;
  rapidjson::Document
  to_json(rapidjson::MemoryPoolAllocator<> *a = nullptr) const;

private:
  Source(std::string sourcename, HDFWriterModule::ptr hdf_writer_module);

  std::string _topic;
  std::string _sourcename;
  std::unique_ptr<HDFWriterModule> _hdf_writer_module;

  uint64_t _processed_messages_count = 0;
  uint64_t _cnt_msg_written = 0;

  friend class CommandHandler;
  friend class FileWriterTask;
  friend class ::Test___FileWriterTask___Create01;
};

} // namespace FileWriter
