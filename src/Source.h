#pragma once

#include "FlatbufferReader.h"
#include "HDFFile.h"
#include "HDFWriterModule.h"
#include "Jemalloc.h"
#include "MMap.h"
#include "Msg.h"
#include "MsgQueue.h"
#include "ProcessMessageResult.h"
#include "SHMP.h"
#include "TimeDifferenceFromMessage.h"
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
  uint64_t processed_messages_count() const;
  void mpi_start(rapidjson::Document config_file, rapidjson::Document command,
                 rapidjson::Document config_stream);
  void mpi_stop();
  ProcessMessageResult process_message(Msg const &msg);
  std::string to_str() const;
  rapidjson::Document
  to_json(rapidjson::MemoryPoolAllocator<> *a = nullptr) const;

private:
  Source(std::string sourcename, HDFWriterModule::ptr hdf_writer_module,
         Jemalloc::sptr, MMap::sptr);

  std::string _topic;
  std::string _sourcename;
  std::unique_ptr<HDFWriterModule> _hdf_writer_module;

  uint64_t _processed_messages_count = 0;
  uint64_t _cnt_msg_written = 0;

  bool do_process_message = true;
  Jemalloc::sptr jm;
  MMap::sptr mmap;
  MsgQueue::ptr queue;
  MPI_Comm comm_spawned;
  MPI_Comm comm_all;
  uint32_t nspawns = 1;
  std::vector<int> mpi_return_codes;

  friend class CommandHandler;
  friend class FileWriterTask;
  friend class SourceFactory_by_FileWriterTask;
  friend std::vector<std::unique_ptr<Source>>
  find_source(rapidjson::Document const &, rapidjson::Document const *);
  friend class ::Test___FileWriterTask___Create01;
  friend void swap(Source &x, Source &y);
};

} // namespace FileWriter
