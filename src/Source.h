#pragma once

#include "Alloc.h"
#include "FlatbufferReader.h"
#include "HDFFile.h"
#include "HDFWriterModule.h"
#include "MMap.h"
#include "MPIChild.h"
#include "Msg.h"
#include "MsgQueue.h"
#include "ProcessMessageResult.h"
#include "SHMP.h"
#include "TimeDifferenceFromMessage.h"
#include "json.h"
#if USE_PARALLEL_WRITER
#include <mpi.h>
#endif
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
                 rapidjson::Document config_stream,
                 std::vector<MPIChild::ptr> &spawns);
  void mpi_stop();
  ProcessMessageResult process_message(Msg &msg);
  std::string to_str() const;
  rapidjson::Document
  to_json(rapidjson::MemoryPoolAllocator<> *a = nullptr) const;

  MsgQueue::ptr queue;

private:
  Source(std::string sourcename, HDFWriterModule::ptr hdf_writer_module,
         Jemalloc::sptr, MMap::sptr, CollectiveQueue *cq);

  std::string _topic;
  std::string _sourcename;
  std::unique_ptr<HDFWriterModule> _hdf_writer_module;

  uint64_t _processed_messages_count = 0;
  uint64_t _cnt_msg_written = 0;

  bool do_process_message = true;
  Jemalloc::sptr jm;
  MMap::sptr mmap;
  CollectiveQueue *cq = nullptr;

  friend class CommandHandler;
  friend class FileWriterTask;
  friend class SourceFactory_by_FileWriterTask;
  friend std::vector<std::unique_ptr<Source>>
  find_source(rapidjson::Document const &, rapidjson::Document const *);
  friend class ::Test___FileWriterTask___Create01;
  friend void swap(Source &x, Source &y);
};

} // namespace FileWriter
