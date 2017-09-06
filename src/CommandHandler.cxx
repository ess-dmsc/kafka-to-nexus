#include "CommandHandler.h"
#include "FileWriterTask.h"
#include "HDFWriterModule.h"
#include "MMap.h"
#include "helper.h"
#include "utils.h"
#include <h5.h>

#include <future>

// getpid()
#include <sys/types.h>
#include <unistd.h>

#include <jemalloc/jemalloc.h>

namespace FileWriter {

std::string find_filename(rapidjson::Document const &d) {
  auto m1 = d.FindMember("file_attributes");
  if (m1 != d.MemberEnd() && m1->value.IsObject()) {
    auto m2 = m1->value.FindMember("file_name");
    if (m2 != m1->value.MemberEnd() && m2->value.IsString()) {
      return m2->value.GetString();
    }
  }
  return std::string{"a-dummy-name.h5"};
}

std::string find_job_id(rapidjson::Document const &d) {
  auto m = d.FindMember("job_id");
  if (m != d.MemberEnd() && m->value.IsString()) {
    return m->value.GetString();
  }
  return std::string{""};
}

std::string find_broker(rapidjson::Document const &d) {
  auto m = d.FindMember("broker");
  if (m != d.MemberEnd() && m->value.IsString()) {
    auto s = std::string(m->value.GetString());
    if (s.substr(0, 2) == "//") {
      uri::URI u(s);
      return u.host_port;
    } else {
      return s;
    }
  }
  return std::string{"localhost:9092"};
}

ESSTimeStamp find_time(rapidjson::Document const &d, const std::string &key) {
  auto m = d.FindMember(key.c_str());
  if (m != d.MemberEnd() && m->value.IsUint64()) {
    return ESSTimeStamp(m->value.GetUint64());
  }
  return ESSTimeStamp{0};
}

// In the future, want to handle many, but not right now.
static int g_N_HANDLED = 0;

CommandHandler::CommandHandler(MainOpt &config, Master *master)
    : config(config), master(master) {
  // Will take care of this in upcoming PR.
  if (false) {
    using namespace rapidjson;
    auto buf1 = gulp("/test/schema-command.json");
    auto doc = make_unique<rapidjson::Document>();
    ParseResult err = doc->Parse(buf1.data(), buf1.size());
    if (err.Code() != ParseErrorCode::kParseErrorNone) {
      LOG(7, "ERROR can not parse schema_command");
      throw std::runtime_error("ERROR can not parse schema_command");
    }
    schema_command.reset(new SchemaDocument(*doc));
  }
}

void jemcb(void *cbd, char const *s) { fwrite(s, 1, strlen(s), stdout); }

void *g_alloc_base = nullptr;
void *g_alloc_max = nullptr;

void *extent_alloc(extent_hooks_t *extent_hooks, void *addr, size_t size,
                   size_t align, bool *zero, bool *commit, unsigned arena) {
  LOG(3, "extent_alloc arena: {}  size: {}  align: {}  zero: {}  commit: {}",
      arena, size, align, *zero, *commit);
  void *q = new char[size];
  if (*zero) {
    std::memset(q, 0, size);
  }
  if (addr != nullptr) {
    LOG(3, "error addr is set");
    exit(1);
  }
  return q;
}

void CommandHandler::handle_new(rapidjson::Document const &d) {
  // if (g_N_HANDLED > 0) return;
  using namespace rapidjson;
  using std::move;
  using std::string;
  if (schema_command) {
    SchemaValidator vali(*schema_command);
    if (!d.Accept(vali)) {
      StringBuffer sb1, sb2;
      vali.GetInvalidSchemaPointer().StringifyUriFragment(sb1);
      vali.GetInvalidDocumentPointer().StringifyUriFragment(sb2);
      LOG(6, "ERROR command message schema validation:  Invalid schema: {}  "
             "keyword: {}",
          sb1.GetString(), vali.GetInvalidSchemaKeyword());
      return;
    }
  }

  {
    // Spawn MPI here.
    MPI_Info mpi_info;
    if (MPI_Info_create(&mpi_info) != MPI_SUCCESS) {
      LOG(3, "ERROR can not init MPI_Info");
      exit(1);
    }
    MPI_Comm comm_spawned;
    std::array<int, 10> mpi_return_codes;
    char arg1[32];
    strncpy(arg1, "--mpi", 5);
    char *argv[] = {
        arg1, nullptr,
    };
    LOG(3, "Spawning as: {}", getpid());
    FILE *f1 = fopen("tmp-pid.txt", "wb");
    auto pidstr = fmt::format("{}", getpid());
    fwrite(pidstr.data(), pidstr.size(), 1, f1);
    fclose(f1);
    // std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    LOG(3, "go on as: {}", getpid());

    uint8_t nspawns = 1;
    auto err =
        MPI_Comm_spawn("./mpi-worker", argv, nspawns, MPI_INFO_NULL, 0,
                       MPI_COMM_WORLD, &comm_spawned, mpi_return_codes.data());
    if (err != MPI_SUCCESS) {
      LOG(3, "can not spawn");
      exit(1);
    }
    {
      int rank, size;
      MPI_Comm_rank(MPI_COMM_WORLD, &rank);
      MPI_Comm_size(MPI_COMM_WORLD, &size);
      LOG(3, "I am rank {} of {}", rank, size);
      if (comm_spawned == MPI_COMM_WORLD) {
        LOG(3, "Weird, child comm is MPI_COMM_WORLD");
      }
    }

    {
      int rank, size;
      MPI_Comm_rank(comm_spawned, &rank);
      MPI_Comm_size(comm_spawned, &size);
      LOG(3, "comm_spawned rank: {}  size: {}", rank, size);
    }

    MPI_Comm comm_all;
    { err = MPI_Intercomm_merge(comm_spawned, 0, &comm_all); }
    {
      int rank, size;
      MPI_Comm_rank(comm_all, &rank);
      MPI_Comm_size(comm_all, &size);
      LOG(3, "comm_all rank: {}  size: {}", rank, size);
    }

    MPI_Barrier(comm_all);
    LOG(3, "mmap");
    auto shm = MMap::create("tmp-mmap", 80 * 1024 * 1024);
    std::memset(shm->addr(), 'a', 1024);

    MPI_Barrier(comm_all);
    LOG(3, "wait for other mmap");

    MPI_Barrier(comm_all);
    LOG(3, "continue");

    auto m1 = (std::atomic<uint32_t> *)shm->addr();
    m1->store(97);
    while (m1->load() < 110) {
      while (m1->load() % 2 == 1) {
      }
      m1->store(m1->load() + 1);
    }

    {
      char const *jemalloc_version = nullptr;
      ;
      size_t n = sizeof(char const *);
      mallctl("version", &jemalloc_version, &n, nullptr, 0);
      LOG(3, "jemalloc version: {}", jemalloc_version);
      unsigned narenas = 0;
      n = sizeof(narenas);
      mallctl("arenas.narenas", &narenas, &n, nullptr, 0);
      LOG(3, "arenas.narenas: {}", narenas);

      // malloc_stats_print(jemcb, nullptr, "");

      extent_hooks_t hooks;
      std::memset(&hooks, 0, sizeof(extent_hooks_t));
      hooks.alloc = extent_alloc;
      extent_hooks_t *hooks_ptr = &hooks;
      unsigned aix = 0;
      n = sizeof(aix);
      int err = mallctl("arenas.create", &aix, &n, &hooks_ptr,
                        sizeof(extent_hooks_t *));
      // int err = mallctl("arenas.create", &aix, &n, nullptr, 0);
      if (err != 0) {
        LOG(3, "error in mallctl arenas.create");
        switch (err) {
        case EINVAL:
          LOG(3, "EINVAL");
          break;
        case ENOENT:
          LOG(3, "ENOENT");
          break;
        case EPERM:
          LOG(3, "EPERM");
          break;
        case EAGAIN:
          LOG(3, "EAGAIN");
          break;
        case EFAULT:
          LOG(3, "EFAULT");
          break;
        }
        exit(1);
      }
      LOG(3, "arena created: {}", aix);

      // void * big = mallocx(80 * 1024 * 1024, MALLOCX_ARENA(aix));

      // malloc_stats_print(jemcb, nullptr, "");
    }

    MPI_Barrier(comm_all);
    LOG(3, "ask for disconnect");
    err = MPI_Comm_disconnect(&comm_spawned);
    if (err != MPI_SUCCESS) {
      LOG(3, "fail MPI_Comm_disconnect");
      exit(1);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    MPI_Finalize();
    exit(1);
  }

  auto fwt = std::unique_ptr<FileWriterTask>(new FileWriterTask);

  auto job_id = find_job_id(d);
  if (!job_id.empty()) {
    fwt->job_id_init(job_id);
  } else {
    LOG(2, "Command not accepted: missing job_id");
    return;
  }

  auto fname = find_filename(d);
  fwt->set_hdf_filename(fname);

  // When FileWriterTask::hdf_init() returns, `stream_hdf_info` will contain
  // the list of streams which have been found in the `nexus_structure`.
  std::vector<StreamHDFInfo> stream_hdf_info;
  std::vector<hid_t> groups;
  {
    rapidjson::Value config_file;
    auto &nexus_structure = d.FindMember("nexus_structure")->value;
    auto x =
        fwt->hdf_init(nexus_structure, config_file, stream_hdf_info, groups);
    if (x) {
      LOG(7, "ERROR hdf init failed, cancel this write command");
      return;
    }
  }

  LOG(6, "Command contains {} streams", stream_hdf_info.size());
  for (auto &stream : stream_hdf_info) {
    auto config_stream_value = get_object(*stream.config_stream, "stream");
    if (!config_stream_value) {
      LOG(5, "Missing stream specification");
      continue;
    }
    auto &config_stream = *config_stream_value.v;
    LOG(7, "Adding stream: {}", json_to_string(config_stream));
    auto topic = get_string(&config_stream, "topic");
    if (!topic) {
      LOG(5, "Missing topic on stream specification");
      continue;
    }
    auto source = get_string(&config_stream, "source");
    if (!source) {
      LOG(5, "Missing source on stream specification");
      continue;
    }
    auto module = get_string(&config_stream, "module");
    if (!module) {
      LOG(5, "Missing module on stream specification");
      continue;
    }

    auto module_factory = HDFWriterModuleRegistry::find(module.v);
    if (!module_factory) {
      LOG(5, "Module '{}' is not available", module.v);
      continue;
    }

    auto hdf_writer_module = module_factory();
    if (!hdf_writer_module) {
      LOG(5, "Can not create a HDFWriterModule for '{}'", module.v);
      continue;
    }

    hdf_writer_module->parse_config(config_stream, nullptr);
    hdf_writer_module->init_hdf(fwt->hdf_file.h5file, stream.hdf_parent_name);
    hdf_writer_module->close();
    hdf_writer_module.reset();
  }

  auto old_fid = fwt->hdf_file.h5file;
  auto print = [](std::string s, hid_t oid) {
    H5O_info_t i;
    H5Oget_info(oid, &i);
    LOG(3, "{} refs: {}", s, i.rc);
  };

  print("before", old_fid);

  std::vector<hid_t> obj_id_list(1024);
  auto nopen = H5Fget_obj_ids(old_fid, H5F_OBJ_ALL, obj_id_list.size(),
                              obj_id_list.data());
  for (int i1 = 0; i1 < nopen; ++i1) {
    auto id = obj_id_list[i1];
    std::vector<char> name(512);
    H5Iget_name(id, name.data(), name.size());
    LOG(3, "{}:  {}", i1, name.data());
    // H5Oclose(id);
  }

  fwt->hdf_filename = fname;
  fwt->hdf_file.reopen(fname, Value());

  /*
  TODO
  - Setup the shared memory arena
  - Test that it really works
  - Init jemalloc
  - Package info about that for the workers
  */

  for (auto &stream : stream_hdf_info) {
    // TODO
    // Refactor with the above loop..
    auto config_stream_value = get_object(*stream.config_stream, "stream");
    if (!config_stream_value) {
      LOG(5, "Missing stream specification");
      continue;
    }
    auto &config_stream = *config_stream_value.v;
    // LOG(7, "Adding stream: {}", json_to_string(config_stream));
    auto topic = get_string(&config_stream, "topic");
    if (!topic) {
      LOG(5, "Missing topic on stream specification");
      continue;
    }
    auto source = get_string(&config_stream, "source");
    if (!source) {
      LOG(5, "Missing source on stream specification");
      continue;
    }
    auto module = get_string(&config_stream, "module");
    if (!module) {
      LOG(5, "Missing module on stream specification");
      continue;
    }

    auto module_factory = HDFWriterModuleRegistry::find(module.v);
    if (!module_factory) {
      LOG(5, "Module '{}' is not available", module.v);
      continue;
    }

    auto hdf_writer_module = module_factory();
    if (!hdf_writer_module) {
      LOG(5, "Can not create a HDFWriterModule for '{}'", module.v);
      continue;
    }

    hdf_writer_module->parse_config(config_stream, nullptr);
    hdf_writer_module->reopen(fwt->hdf_file.h5file, stream.hdf_parent_name);

    // for each stream:
    //   either re-open in this main process
    //     Re-create HDFWriterModule
    //     Re-parse the stream config
    //     Re-open HDF items
    //     Create a Source which feeds directly to that module
    auto s = Source(source.v, move(hdf_writer_module));
    s._topic = string(topic);
    s.do_process_message = config.source_do_process_message;
    fwt->add_source(move(s));

    //   or re-open in one or more separate mpi workers
    //     Send command to create HDFWriterModule, all the json config as text,
    //     let it re-open hdf items
    //     Create a Source which puts messages on a queue
  }

  if (master) {
    auto br = find_broker(d);
    auto config_kafka = config.kafka;
    std::vector<std::pair<string, string>> config_kafka_vec;
    for (auto &x : config_kafka) {
      config_kafka_vec.emplace_back(x.first, x.second);
    }

    auto s = std::unique_ptr<StreamMaster<Streamer>>(
        new StreamMaster<Streamer>(br, std::move(fwt), config_kafka_vec));
    if (master->status_producer) {
      s->report(master->status_producer, config.status_master_interval);
    }
    auto start_time = find_time(d, "start_time");
    if (start_time.count()) {
      LOG(6, "start time :\t{}", start_time.count());
      s->start_time(start_time);
    }
    auto stop_time = find_time(d, "stop_time");
    if (stop_time.count()) {
      LOG(6, "stop time :\t{}", stop_time.count());
      s->stop_time(stop_time);
    }
    s->start();
    master->stream_masters.push_back(std::move(s));
  } else {
    file_writer_tasks.emplace_back(std::move(fwt));
  }
  g_N_HANDLED += 1;
}

void CommandHandler::handle_file_writer_task_clear_all(
    rapidjson::Document const &d) {
  using namespace rapidjson;
  if (master) {
    for (auto &x : master->stream_masters) {
      x->stop();
    }
  }
  file_writer_tasks.clear();
}

void CommandHandler::handle_exit(rapidjson::Document const &d) {
  if (master)
    master->stop();
}

void CommandHandler::handle_stream_master_stop(rapidjson::Document const &d) {
  if (master) {
    auto s = get_string(&d, "job_id");
    auto job_id = std::string(s);
    ESSTimeStamp stop_time{0};
    {
      auto m = d.FindMember("stop_time");
      if (m != d.MemberEnd()) {
        stop_time = ESSTimeStamp(m->value.GetUint64());
      }
    }
    int counter{0};
    for (auto &x : master->stream_masters) {
      if (x->job_id() == job_id) {
        if (stop_time.count()) {
          x->stop_time(stop_time);
        } else {
          x->stop();
        }
        LOG(5, "gracefully stop file with id : {}", job_id);
        ++counter;
      }
    }

    if (counter == 0) {
      LOG(3, "no file with id : {}", job_id);
    } else if (counter > 1) {
      LOG(3, "error: multiple files with id : {}", job_id);
    }
  }
}

void CommandHandler::handle(rapidjson::Document const &d) {
  using std::string;
  using namespace rapidjson;
  uint64_t teamid = 0;
  uint64_t cmd_teamid = 0;
  if (master) {
    teamid = master->config.teamid;
  }
  if (auto i = get_int(&d, "teamid")) {
    cmd_teamid = int64_t(i);
  }
  if (cmd_teamid != teamid) {
    LOG(1, "INFO command is for teamid {:016x}, we are {:016x}", cmd_teamid,
        teamid);
    return;
  }

  // The ways to give commands will be unified in upcoming PR.
  if (auto s = get_string(&d, "cmd")) {
    auto cmd = string(s);
    if (cmd == "FileWriter_new") {
      handle_new(d);
      return;
    }
    if (cmd == "FileWriter_exit") {
      handle_exit(d);
      return;
    }
    if (cmd == "FileWriter_stop") {
      handle_stream_master_stop(d);
      return;
    }
  }

  if (auto s = get_string(&d, "recv_type")) {
    auto recv_type = string(s);
    if (recv_type == "FileWriter") {
      if (auto s = get_string(&d, "cmd")) {
        auto cmd = string(s);
        if (cmd == "file_writer_tasks_clear_all") {
          handle_file_writer_task_clear_all(d);
          return;
        }
      }
    }
  }

  StringBuffer buffer;
  PrettyWriter<StringBuffer> writer(buffer);
  d.Accept(writer);
  LOG(3, "ERROR could not figure out this command: {}", buffer.GetString());
}

void CommandHandler::handle(Msg const &msg) {
  using std::string;
  using namespace rapidjson;
  auto doc = make_unique<Document>();
  ParseResult err = doc->Parse((char *)msg.data(), msg.size());
  if (doc->HasParseError()) {
    LOG(2, "ERROR json parse: {} {}", err.Code(), GetParseError_En(err.Code()));
    return;
  }
  handle(*doc);
}

} // namespace FileWriter
