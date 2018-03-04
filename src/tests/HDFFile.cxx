#include "../HDFFile.h"
#include "../Alloc.h"
#include "../CommandHandler.h"
#include "../KafkaW/KafkaW.h"
#include "../MainOpt.h"
#include "../helper.h"
#include "../schemas/ev42/ev42_synth.h"
#include "../schemas/f142/f142_synth.h"
#include <array>
#include <chrono>
#include <gtest/gtest.h>
#include <hdf5.h>
#include <random>
#include <rapidjson/document.h>
#include <string>
#include <unistd.h>
#include <vector>

using std::array;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::string;
using std::vector;

void merge_config_into_main_opt(MainOpt &main_opt, string jsontxt) {
  rapidjson::Document cfg;
  cfg.Parse(jsontxt.c_str());
  main_opt.config_file = merge(cfg, main_opt.config_file);
}

rapidjson::Document basic_command(string filename) {
  rapidjson::Document cmd;
  auto &a = cmd.GetAllocator();
  cmd.Parse(R""({
    "cmd": "FileWriter_new",
    "nexus_structure": {
      "children": []
    },
    "file_attributes": {
    },
    "job_id": "some_unique_id"
  })"");
  cmd.FindMember("file_attributes")
      ->value.GetObject()
      .AddMember("file_name", rapidjson::Value(filename.c_str(), a), a);
  return cmd;
}

void command_add_static_dataset_1d(rapidjson::Document &cmd) {
  auto &a = cmd.GetAllocator();
  rapidjson::Document j(&a);
  j.Parse(R""({
    "type": "group",
    "name": "some_group",
    "attributes": {
      "NX_class": "NXinstrument"
    },
    "children": [
      {
        "type": "dataset",
        "name": "value",
        "values": 42.24,
        "attributes": {"units":"degree"}
      }
    ]
  })"");
  cmd.FindMember("nexus_structure")
      ->value.GetObject()
      .FindMember("children")
      ->value.GetArray()
      .PushBack(j, a);
}

void send_stop(FileWriter::CommandHandler &ch, rapidjson::Value &job_cmd) {
  string cmd = fmt::format(R""({{
    "recv_type": "FileWriter",
    "cmd": "file_writer_tasks_clear_all",
    "job_id": "{}"
  }})"",
                           job_cmd.FindMember("job_id")->value.GetString());
  ch.handle(FileWriter::Msg::owned(cmd.data(), cmd.size()));
}

// Verify
TEST(HDFFile, create) {
  auto fname = "tmp-test.h5";
  unlink(fname);
  using namespace FileWriter;
  HDFFile f1;
  std::vector<StreamHDFInfo> stream_hdf_info;
  f1.init("tmp-test.h5", rapidjson::Value().SetObject(),
          rapidjson::Value().SetObject(), stream_hdf_info);
}

class T_CommandHandler : public testing::Test {
public:
  static void new_03() {
    auto cmd = gulp("tests/msg-cmd-new-03.json");
    LOG(Sev::Debug, "cmd: {:.{}}", cmd.data(), cmd.size());
    rapidjson::Document d;
    d.Parse(cmd.data(), cmd.size());
    char const *fname = d["file_attributes"]["file_name"].GetString();
    unlink(fname);
    MainOpt main_opt;
    FileWriter::CommandHandler ch(main_opt, nullptr);
    ch.handle(FileWriter::Msg::owned(cmd.data(), cmd.size()));
  }

  static bool check_cue(std::vector<uint64_t> const &event_time_zero,
                        std::vector<uint32_t> const &event_index,
                        uint64_t cue_timestamp_zero, uint32_t cue_index) {
    bool found = false;
    size_t i2 = 0;
    for (auto &evt : event_time_zero) {
      if (evt == cue_timestamp_zero) {
        found = true;
        break;
      }
      ++i2;
    }
    if (!found) {
      return false;
    }
    return event_index[i2] == cue_index;
  }

  static void create_static_file_with_hdf_output_prefix() {
    MainOpt &main_opt = *g_main_opt.load();
    std::string const hdf_output_prefix = "tmp-relative-output";
    std::string const hdf_output_filename = "tmp-file-with-hdf-prefix.h5";
#ifdef _MSC_VER
#else
    mkdir(hdf_output_prefix.c_str(), 0777);
    unlink((hdf_output_prefix + "/" + hdf_output_filename).c_str());
#endif
    {
      std::string jsontxt =
          fmt::format(R""({{"hdf-output-prefix": "{}"}})"", hdf_output_prefix);
      merge_config_into_main_opt(main_opt, jsontxt);
      main_opt.hdf_output_prefix = hdf_output_prefix;
    }
    rapidjson::Document json_command = basic_command(hdf_output_filename);
    command_add_static_dataset_1d(json_command);

    auto cmd = json_to_string(json_command);
    auto fname = get_string(&json_command, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), 8u);

    FileWriter::CommandHandler ch(main_opt, nullptr);
    ch.handle(FileWriter::Msg::owned(cmd.data(), cmd.size()));
    ASSERT_EQ(ch.FileWriterTasks.size(), (size_t)1);
    send_stop(ch, json_command);
    ASSERT_EQ(ch.FileWriterTasks.size(), (size_t)0);
    main_opt.hdf_output_prefix = "";

    // Verification
    auto file = hdf5::file::open(hdf_output_prefix + "/" + fname.v,
                                 hdf5::file::AccessFlags::READONLY);
    ASSERT_TRUE(file.is_valid());
  }

  static void create_static_dataset() {
    MainOpt &main_opt = *g_main_opt.load();
    merge_config_into_main_opt(main_opt, R""({})"");
    std::string const hdf_output_filename = "tmp-static-dataset.h5";
    unlink(hdf_output_filename.c_str());
    rapidjson::Document json_command;
    {
      using namespace rapidjson;
      auto &j = json_command;
      auto &a = j.GetAllocator();
      j.SetObject();
      Value nexus_structure;
      nexus_structure.SetObject();

      Value children;
      children.SetArray();

      {
        Value g1;
        g1.SetObject();
        g1.AddMember("type", "group", a);
        g1.AddMember("name", "some_group", a);
        Value attr;
        attr.SetObject();
        attr.AddMember("NX_class", "NXinstrument", a);
        g1.AddMember("attributes", attr, a);
        Value ch;
        ch.SetArray();
        {
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "value",
                  "values": 42.24,
                  "attributes": {"units":"degree"}
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "more_complex_set",
                  "dataset": {
                    "space": "simple",
                    "type": "double",
                    "size": ["unlimited", 2]
                  },
                  "values": [
                    [13.1, 14]
                  ]
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "string_scalar",
                  "dataset": {
                    "type": "string"
                  },
                  "values": "the-scalar-string"
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "string_1d",
                  "dataset": {
                    "type": "string",
                    "size": ["unlimited"]
                  },
                  "values": ["the-scalar-string", "another-one"]
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "string_2d",
                  "dataset": {
                    "type": "string",
                    "size": ["unlimited", 2]
                  },
                  "values": [
                    ["the-scalar-string", "another-one"],
                    ["string_1_0", "string_1_1"]
                  ]
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "string_3d",
                  "dataset": {
                    "type": "string",
                    "size": ["unlimited", 3, 2]
                  },
                  "values": [
                    [
                      ["string_0_0_0", "string_0_0_1"],
                      ["string_0_1_0", "string_0_1_1"],
                      ["string_0_2_0", "string_0_2_1"]
                    ],
                    [
                      ["string_1_0_0", "string_1_0_1"],
                      ["string_1_1_0", "string_1_1_1"],
                      ["string_1_2_0", "string_1_2_1"]
                    ]
                  ]
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "string_fixed_1d",
                  "dataset": {
                    "type":"string",
                    "string_size": 32,
                    "size": ["unlimited"]
                  },
                  "values": ["the-scalar-string", "another-one"]
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({"type":"dataset", "name": "big_set", "dataset": {"space":"simple", "type":"double", "size":["unlimited", 4, 2]}})"");
            {
              Value values;
              values.SetArray();
              for (size_t i1 = 0; i1 < 7; ++i1) {
                Value v1;
                v1.SetArray();
                for (size_t i2 = 0; i2 < 4; ++i2) {
                  Value v2;
                  v2.SetArray();
                  for (size_t i3 = 0; i3 < 2; ++i3) {
                    v2.PushBack(Value().SetInt(1000 * i1 + 10 * i2 + i3), a);
                  }
                  v1.PushBack(v2, a);
                }
                values.PushBack(v1, a);
              }
              jd.AddMember("values", values, a);
            }
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
        }
        g1.AddMember("children", ch, a);
        children.PushBack(g1, a);
      }
      nexus_structure.AddMember("children", children, a);
      j.AddMember("nexus_structure", nexus_structure, a);
      {
        Value v;
        v.SetObject();
        v.AddMember("file_name", StringRef(hdf_output_filename.c_str()), a);
        j.AddMember("file_attributes", v, a);
      }
      j.AddMember("cmd", StringRef("FileWriter_new"), a);
      j.AddMember("job_id", StringRef("000000000dataset"), a);
    }

    auto cmd = json_to_string(json_command);
    auto fname = get_string(&json_command, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), 8u);

    FileWriter::CommandHandler ch(main_opt, nullptr);
    ch.handle(FileWriter::Msg::owned(cmd.data(), cmd.size()));
    ASSERT_EQ(ch.FileWriterTasks.size(), (size_t)1);
    send_stop(ch, json_command);
    ASSERT_EQ(ch.FileWriterTasks.size(), (size_t)0);

    // Verification
    auto file =
        hdf5::file::open(string(fname), hdf5::file::AccessFlags::READONLY);
    auto ds = hdf5::node::get_dataset(file.root(), "/some_group/value");
    ASSERT_EQ(ds.datatype(), hdf5::datatype::create<double>());
    ASSERT_TRUE(ds.attributes["units"].is_valid());
  }

  static void write_attributes_at_top_level_of_the_file() {
    MainOpt &main_opt = *g_main_opt.load();
    merge_config_into_main_opt(main_opt, R""({})"");
    std::string const hdf_output_filename = "tmp_write_top_level_attributes.h5";
    unlink(hdf_output_filename.c_str());
    rapidjson::Document json_command;
    json_command.Parse(R""({
      "cmd": "FileWriter_new",
      "nexus_structure": {
        "attributes": {
          "some_top_level_int": 42,
          "some_top_level_string": "Hello Attribute"
        }
      },
      "file_attributes": {
      },
      "job_id": "832yhtwgfskdf"
    })"");
    json_command.FindMember("file_attributes")
        ->value.GetObject()
        .AddMember("file_name", rapidjson::Value(hdf_output_filename.c_str(),
                                                 json_command.GetAllocator()),
                   json_command.GetAllocator());
    auto cmd = json_to_string(json_command);
    auto fname = get_string(&json_command, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), 8u);

    FileWriter::CommandHandler ch(main_opt, nullptr);
    ch.handle(FileWriter::Msg::owned(cmd.data(), cmd.size()));
    ASSERT_EQ(ch.FileWriterTasks.size(), (size_t)1);
    send_stop(ch, json_command);
    ASSERT_EQ(ch.FileWriterTasks.size(), (size_t)0);

    // Verification
    auto file =
        hdf5::file::open(string(fname), hdf5::file::AccessFlags::READONLY);
    auto root_group = file.root();
    {
      auto attr = root_group.attributes["some_top_level_int"];
      ASSERT_EQ(attr.datatype().get_class(), hdf5::datatype::Class::INTEGER);
      uint32_t v = 0;
      attr.read(v);
      ASSERT_EQ(v, 42u);
    }
    {
      auto attr = root_group.attributes["some_top_level_string"];
      ASSERT_EQ(attr.datatype().get_class(), hdf5::datatype::Class::STRING);
      std::string val;
      attr.read(val, attr.datatype());
      ASSERT_EQ(string("Hello Attribute"), val);
    }
    {
      auto attr = root_group.attributes["HDF5_Version"];
      ASSERT_EQ(attr.datatype().get_class(), hdf5::datatype::Class::STRING);
      std::string val;
      attr.read(val, attr.datatype());
      ASSERT_EQ(FileWriter::HDFFile::h5_version_string_linked(), val);
    }
    {
      auto attr = root_group.attributes["file_time"];
      ASSERT_EQ(attr.datatype().get_class(), hdf5::datatype::Class::STRING);
    }
  }

  /// Can supply pre-generated test data for a source on a topic to profile
  /// the writing.
  class SourceDataGen {
  public:
    string topic;
    string source;
    uint64_t seed = 0;
    std::mt19937 rnd;
    vector<FlatBufs::ev42::fb> fbs;
    vector<FileWriter::Msg> msgs;
    // Number of messages already fed into file writer during testing
    size_t n_fed = 0;
    bool run_parallel = false;
    int n_events_per_message = 0;
    /// Generates n test messages which we can later feed from memory into the
    /// file writer.
    void pregenerate(int n, int n_events_per_message_,
                     std::shared_ptr<Alloc> &jm) {
      n_events_per_message = n_events_per_message_;
      LOG(Sev::Debug, "generating {} {}...", topic, source);
      FlatBufs::ev42::synth synth(source, seed);
      rnd.seed(seed);
      for (int i1 = 0; i1 < n; ++i1) {
        // Number of events per message:
        // size_t n_ele = rnd() >> 24;
        // Currently fixed, have to adapt verification code first.
        auto n_ele = n_events_per_message;
        fbs.push_back(synth.next(n_ele));
        auto &fb = fbs.back();

        // Allocate memory on JM AND CHECK IT!
        msgs.push_back(FileWriter::Msg::shared(
            (char const *)fb.builder->GetBufferPointer(), fb.builder->GetSize(),
            jm));
        if (msgs.back().size() < 8) {
          LOG(Sev::Error, "error");
          exit(1);
        }
      }
    }
  };

  static void recreate_file(rapidjson::Value *json_command) {
    // now try to recreate the file for testing:
    auto m = json_command->FindMember("file_attributes");
    auto fn = m->value.GetObject().FindMember("file_name")->value.GetString();
    auto file = hdf5::file::create(fn, hdf5::file::AccessFlags::TRUNCATE);
  }

  /// Used by `data_ev42` test to verify attributes attached to the group.
  static void verify_attribute_data_ev42(hdf5::node::Group &node) {

    auto a1 = node.attributes["this_will_be_a_double"];
    auto dt = a1.datatype();
    ASSERT_EQ(dt.get_class(), hdf5::datatype::Class::FLOAT);
    ASSERT_EQ(dt.size(), sizeof(double));
    double v;
    a1.read(v);
    ASSERT_EQ(v, 0.125);
  }

  static void data_ev42() {
    MainOpt &main_opt = *g_main_opt.load();
    bool do_verification = true;

    // Defaults such that the test has a chance to succeed
    merge_config_into_main_opt(main_opt, R""({
      "nexus": {
        "indices": {
          "index_every_kb": 1
        },
        "chunk": {
          "chunk_kb": 1024
        },
        "buffer": {
          "size_kb": 512,
          "packet_max_kb": 128
        }
      },
      "unit_test": {
        "n_events_per_message": 16,
        "n_msgs_per_source": 32,
        "n_sources": 1,
        "n_mpi_workers": 1,
        "feed_msgs_seconds": 30,
        "filename": "tmp-ev42.h5",
        "hdf": {
          "do_verification": 1,
          "do_recreate": 0
        }
      },
      "shm": {
        "fname": "tmp-mmap"
      },
      "mpi": {
        "path_bin": "."
      },
      "shm": 2100100100
    })"");

    if (auto x =
            get_uint(&main_opt.config_file, "unit_test.hdf.do_verification")) {
      do_verification = x.v == 1;
      LOG(Sev::Debug, "do_verification: {}", do_verification);
    }

    size_t n_msgs_per_source = 1;
    if (auto x =
            get_uint(&main_opt.config_file, "unit_test.n_msgs_per_source")) {
      LOG(Sev::Debug, "unit_test.n_msgs_per_source: {}", x.v);
      n_msgs_per_source = x.v;
    }

    size_t n_sources = 1;
    if (auto x = get_uint(&main_opt.config_file, "unit_test.n_sources")) {
      LOG(Sev::Debug, "unit_test.n_sources: {}", x.v);
      n_sources = x.v;
    }

    size_t n_events_per_message = 1;
    if (auto x =
            get_uint(&main_opt.config_file, "unit_test.n_events_per_message")) {
      LOG(Sev::Debug, "unit_test.n_events_per_message: {}", x.v);
      n_events_per_message = x.v;
    }

    size_t feed_msgs_times = 1;
    if (auto x = get_uint(&main_opt.config_file, "unit_test.feed_msgs_times")) {
      LOG(Sev::Info, "unit_test.feed_msgs_times: {}", x.v);
      feed_msgs_times = x.v;
    }

    int feed_msgs_seconds = 1;
    if (auto x =
            get_int(&main_opt.config_file, "unit_test.feed_msgs_seconds")) {
      LOG(Sev::Info, "unit_test.feed_msgs_seconds: {}", x.v);
      feed_msgs_seconds = x.v;
    }

    string filename = "tmp-ev42.h5";
    if (auto x = get_string(&main_opt.config_file, "unit_test.filename")) {
      LOG(Sev::Info, "unit_test.filename: {}", x.v);
      filename = x.v;
    }

    auto &jm = main_opt.jm;
    vector<SourceDataGen> sources;
    for (size_t i1 = 0; i1 < n_sources; ++i1) {
      sources.emplace_back();
      auto &s = sources.back();
      // Currently, we assume only one topic!
      s.topic = "topic.with.multiple.sources";
      s.source = fmt::format("for_example_motor_{:04}", i1);
      s.run_parallel = true;
      s.pregenerate(n_msgs_per_source, n_events_per_message, jm);
    }
    if (false) {
      vector<std::thread> threads_pregen;
      for (size_t i1 = 0; i1 < n_sources; ++i1) {
        auto &s = sources.back();
        LOG(Sev::Debug, "push pregen {}", i1);
        threads_pregen.push_back(
            std::thread([&jm, &s, n_msgs_per_source, n_events_per_message] {
              s.pregenerate(n_msgs_per_source, n_events_per_message, jm);
            }));
      }
      for (auto &x : threads_pregen) {
        LOG(Sev::Debug, "join pregen");
        x.join();
      }
    }

    if (true) {
      sources.emplace_back();
      auto &s = sources.back();
      s.topic = "topic.with.multiple.sources";
      s.source = fmt::format("stream_for_main_thread_{:04}", 0);
      s.pregenerate(17, 71, jm);
    }

    rapidjson::Document json_command;
    {
      using namespace rapidjson;
      auto &j = json_command;
      auto &a = j.GetAllocator();
      j.SetObject();
      Value nexus_structure;
      nexus_structure.SetObject();

      Value children;
      children.SetArray();

      {
        Value g1;
        g1.SetObject();
        g1.AddMember("type", "group", a);
        g1.AddMember("name", "some_group", a);
        Value attr;
        attr.SetObject();
        attr.AddMember("NX_class", "NXinstrument", a);
        g1.AddMember("attributes", attr, a);
        Value ch;
        ch.SetArray();
        g1.AddMember("children", ch, a);
        children.PushBack(g1, a);
      }

      auto json_stream = [&a, &main_opt](string source, string topic,
                                         string module,
                                         bool run_parallel) -> Value {
        Value g1;
        g1.SetObject();
        g1.AddMember("type", "group", a);
        g1.AddMember("name", Value(source.c_str(), a), a);
        Value attr;
        attr.SetObject();
        attr.AddMember("NX_class", "NXinstrument", a);
        g1.AddMember("attributes", attr, a);
        Value ch;
        ch.SetArray();
        {
          auto &children = ch;
          Document ds1(&a);
          ds1.Parse(R""({
            "type": "stream",
            "attributes": {
              "this_will_be_a_double": 0.125,
              "this_will_be_a_int64": 123
            }
          })"");
          Value stream;
          stream.SetObject();
          if (auto main_nexus = get_object(main_opt.config_file, "nexus")) {
            Value nx;
            nx.CopyFrom(*main_nexus.v, a);
            stream.AddMember("nexus", std::move(nx), a);
          }
          stream.AddMember("topic", Value(topic.c_str(), a), a);
          stream.AddMember("source", Value(source.c_str(), a), a);
          stream.AddMember("writer_module", Value(module.c_str(), a), a);
          stream.AddMember("type", Value("uint32", a), a);
          stream.AddMember(
              "n_mpi_workers",
              std::move(Value().CopyFrom(
                  main_opt.config_file["unit_test"]["n_mpi_workers"], a)),
              a);
          stream.AddMember("run_parallel", Value(run_parallel), a);
          ds1.AddMember("stream", stream, a);
          children.PushBack(ds1, a);
        }
        g1.AddMember("children", ch, a);
        return g1;
      };

      for (auto &source : sources) {
        children.PushBack(json_stream(source.source, source.topic, "ev42",
                                      source.run_parallel),
                          a);
      }

      nexus_structure.AddMember("children", children, a);
      j.AddMember("nexus_structure", nexus_structure, a);
      {
        Value v;
        v.SetObject();
        v.AddMember("file_name", Value(filename.c_str(), a), a);
        j.AddMember("file_attributes", v, a);
      }
      j.AddMember("cmd", StringRef("FileWriter_new"), a);
      j.AddMember("job_id", StringRef("000000000042"), a);
    }

    auto cmd = json_to_string(json_command);
    LOG(Sev::Debug, "command: {}", cmd);

    auto &d = json_command;
    auto fname = get_string(&d, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), size_t{8});

    FileWriter::CommandHandler ch(main_opt, nullptr);

    using DT = uint32_t;
    std::mt19937 rnd_nn;

    for (int file_i = 0; file_i < 1; ++file_i) {
      unlink(string(fname).c_str());

      ch.handle(FileWriter::Msg::owned((char const *)cmd.data(), cmd.size()));
      ASSERT_EQ(ch.FileWriterTasks.size(), (size_t)1);

      auto &fwt = ch.FileWriterTasks.at(0);
      ASSERT_EQ(fwt->demuxers().size(), (size_t)1);

      LOG(Sev::Debug, "processing...");
      using CLK = std::chrono::steady_clock;
      using MS = std::chrono::milliseconds;
      bool do_run = true;
      auto feed_start = CLK::now();
      auto t1 = CLK::now();
      for (size_t i_feed = 0; do_run and i_feed < feed_msgs_times; ++i_feed) {
        size_t i_source = 0;
        for (auto &source : sources) {
          if (not do_run) {
            break;
          }
          if (i_feed % 100 == 0) {
            LOG(Sev::Debug, "i_feed: {:3}  i_source: {:2}", i_feed, i_source);
          }
          for (auto &msg : source.msgs) {
            if (false) {
              auto v = binary_to_hex(msg.data(), msg.size());
              LOG(Sev::Debug, "msg:\n{:.{}}", v.data(), v.size());
            }
            if (msg.size() < 8) {
              LOG(Sev::Error, "error");
              do_run = false;
            }
            auto res = fwt->demuxers().at(0).process_message(
                FileWriter::Msg::cheap(msg, jm));
            if (res.is_ERR()) {
              LOG(Sev::Error, "is_ERR");
              do_run = false;
              break;
            }
            if (res.is_ALL_SOURCES_FULL()) {
              LOG(Sev::Error, "is_ALL_SOURCES_FULL");
              do_run = false;
              break;
            }
            if (res.is_STOP()) {
              LOG(Sev::Error, "is_STOP");
              do_run = false;
              break;
            }
            source.n_fed++;
          }
          i_source += 1;
        }
        auto now = CLK::now();
        if (duration_cast<MS>(now - feed_start).count() / 1000 >=
            feed_msgs_seconds) {
          break;
        }
      }
      auto t2 = CLK::now();
      LOG(Sev::Debug, "processing done in {} ms",
          duration_cast<MS>(t2 - t1).count());
      LOG(Sev::Debug, "finishing...");
      send_stop(ch, json_command);
      ASSERT_EQ(ch.FileWriterTasks.size(), (size_t)0);
      auto t3 = CLK::now();
      LOG(Sev::Debug, "finishing done in {} ms",
          duration_cast<MS>(t3 - t2).count());
      LOG(Sev::Debug, "done in total {} ms",
          duration_cast<MS>(t3 - t1).count());
    }

    if (!do_verification) {
      return;
    }

    size_t minimum_expected_entries_in_the_index = 1;

    auto file =
        hdf5::file::open(string(fname), hdf5::file::AccessFlags::READONLY);
    auto root_group = file.root();

    size_t i_source = 0;
    for (auto &source : sources) {
      vector<DT> data((size_t)(source.n_events_per_message));
      string group_path = "/" + source.source;

      auto ds = hdf5::node::get_dataset(root_group, group_path + "/event_id");

      // LOG(Sev::Debug, "have {} messages", source.msgs.size());
      for (size_t feed_i = 0; feed_i < feed_msgs_times; ++feed_i) {
        for (size_t msg_i = 0; msg_i < source.msgs.size(); ++msg_i) {
          hsize_t i_pos =
              msg_i * source.n_events_per_message +
              feed_i * source.n_events_per_message * source.msgs.size();

          ds.read(data,
                  hdf5::dataspace::Hyperslab(
                      {i_pos},
                      {static_cast<hsize_t>(source.n_events_per_message)}));

          auto fbd = source.fbs.at(msg_i).root()->detector_id();
          for (int i1 = 0; i1 < source.n_events_per_message; ++i1) {
            // LOG(Sev::Debug, "found: {:4}  {:6} vs {:6}", i1, data.at(i1),
            // fbd->Get(i1));
            ASSERT_EQ(data.at(i1), fbd->Get(i1));
          }
        }
      }

      auto ds_cue_timestamp_zero = hdf5::node::get_dataset(
          root_group, group_path + "/cue_timestamp_zero");
      vector<uint64_t> cue_timestamp_zero(
          ds_cue_timestamp_zero.dataspace().size());
      ds_cue_timestamp_zero.read(cue_timestamp_zero);

      auto ds_cue_index =
          hdf5::node::get_dataset(root_group, group_path + "/cue_index");
      vector<uint32_t> cue_index(ds_cue_index.dataspace().size());
      ds_cue_index.read(cue_index);

      ASSERT_GE(cue_timestamp_zero.size(),
                minimum_expected_entries_in_the_index);
      ASSERT_EQ(cue_timestamp_zero.size(), cue_index.size());

      auto ds_event_time_zero =
          hdf5::node::get_dataset(root_group, group_path + "/event_time_zero");
      vector<uint64_t> event_time_zero(ds_event_time_zero.dataspace().size());
      ds_event_time_zero.read(event_time_zero);

      auto ds_event_index =
          hdf5::node::get_dataset(root_group, group_path + "/event_index");
      vector<uint32_t> event_index(ds_event_index.dataspace().size());
      ds_event_index.read(event_index);

      ASSERT_GT(event_time_zero.size(), 0u);
      ASSERT_EQ(event_time_zero.size(), event_index.size());

      for (hsize_t i1 = 0; false && i1 < cue_timestamp_zero.size(); ++i1) {
        auto ok = check_cue(event_time_zero, event_index,
                            cue_timestamp_zero[i1], cue_index[i1]);
        ASSERT_TRUE(ok);
      }

      ++i_source;

      auto attr_node = hdf5::node::get_group(root_group, group_path);
      verify_attribute_data_ev42(attr_node);
    }

    LOG(Sev::Debug, "data_ev42 verification done");
  }

  /// Can supply pre-generated test data for a source on a topic to profile
  /// the writing.
  class SourceDataGen_f142 {
  public:
    string topic;
    string source;
    uint64_t seed = 0;
    std::mt19937 rnd;
    vector<FlatBufs::f142::fb> fbs;
    vector<FileWriter::Msg> msgs;
    // Number of messages already fed into file writer during testing
    size_t n_fed = 0;
    /// Generates n test messages which we can later feed from memory into the
    /// file writer.
    void pregenerate(size_t array_size, uint64_t n,
                     std::shared_ptr<Alloc> &jm) {
      LOG(Sev::Debug, "generating {} {}...", topic, source);
      auto ty = FlatBufs::f142::Value::Double;
      if (array_size > 0) {
        ty = FlatBufs::f142::Value::ArrayFloat;
      }
      FlatBufs::f142::synth synth(source, ty);
      rnd.seed(seed);
      for (uint64_t i1 = 0; i1 < n; ++i1) {
        // Number of events per message:
        // size_t n_ele = rnd() >> 24;
        // Currently fixed, have to adapt verification code first.
        fbs.push_back(synth.next(i1, array_size));
        auto &fb = fbs.back();
        msgs.push_back(FileWriter::Msg::shared(
            (char const *)fb.builder->GetBufferPointer(), fb.builder->GetSize(),
            jm));
      }
    }
  };

  static void data_f142() {
    MainOpt &main_opt = *g_main_opt.load();
    bool do_verification = true;

    // Defaults such that the test has a chance to succeed
    merge_config_into_main_opt(main_opt, R""({
      "nexus": {
        "chunk": {
          "chunk_kb": 1024
        }
      },
      "unit_test": {
        "f142_array_size": 7,
        "n_msgs_per_source": 43,
        "n_sources": 1
      }
    })"");

    if (auto x =
            get_int(&main_opt.config_file, "unit_test.hdf.do_verification")) {
      do_verification = x.v == 1;
      LOG(Sev::Debug, "do_verification: {}", do_verification);
    }

    int n_msgs_per_source = 1;
    if (auto x =
            get_int(&main_opt.config_file, "unit_test.n_msgs_per_source")) {
      LOG(Sev::Debug, "unit_test.n_msgs_per_source: {}", x.v);
      n_msgs_per_source = int(x.v);
    }

    int n_sources = 1;
    if (auto x = get_int(&main_opt.config_file, "unit_test.n_sources")) {
      LOG(Sev::Debug, "unit_test.n_sources: {}", x.v);
      n_sources = int(x.v);
    }

    size_t array_size = 0;
    if (auto x = get_int(&main_opt.config_file, "unit_test.f142_array_size")) {
      LOG(Sev::Debug, "unit_test.f142_array_size: {}", x.v);
      array_size = size_t(x.v);
    }

    vector<SourceDataGen_f142> sources;
    for (int i1 = 0; i1 < n_sources; ++i1) {
      sources.emplace_back();
      auto &s = sources.back();
      // Currently, we assume only one topic!
      s.topic = "topic.with.multiple.sources";
      s.source = fmt::format("for_example_motor_{:04}", i1);
      s.pregenerate(array_size, n_msgs_per_source, main_opt.jm);
    }

    if (false) {
      for (auto &source : sources) {
        LOG(Sev::Debug, "msgs: {}  {}", source.source, source.msgs.size());
      }
    }

    rapidjson::Document json_command;
    {
      using namespace rapidjson;
      auto &j = json_command;
      auto &a = j.GetAllocator();
      j.SetObject();
      Value nexus_structure;
      nexus_structure.SetObject();

      Value children;
      children.SetArray();

      {
        Value g1;
        g1.SetObject();
        g1.AddMember("type", "group", a);
        g1.AddMember("name", "some_group", a);
        Value attr;
        attr.SetObject();
        attr.AddMember("NX_class", "NXinstrument", a);
        g1.AddMember("attributes", attr, a);
        Value ch;
        ch.SetArray();
        {
          auto &children = ch;
          Value ds1;
          ds1.SetObject();
          ds1.AddMember("type", "dataset", a);
          ds1.AddMember("name", "created_by_filewriter", a);
          Value attr;
          attr.SetObject();
          attr.AddMember("NX_class", "NXdetector", a);
          attr.AddMember("this_will_be_a_double", Value(0.123), a);
          attr.AddMember("this_will_be_a_int64", Value(123), a);
          ds1.AddMember("attributes", attr, a);
          Value dataset;
          dataset.SetObject();
          dataset.AddMember("space", "simple", a);
          dataset.AddMember("type", "uint64", a);
          Value dataset_size;
          dataset_size.SetArray();
          dataset_size.PushBack("unlimited", a);
          dataset_size.PushBack(Value(4), a);
          dataset_size.PushBack(Value(2), a);
          dataset.AddMember("size", dataset_size, a);
          ds1.AddMember("dataset", dataset, a);
          children.PushBack(ds1, a);
        }
        g1.AddMember("children", ch, a);
        children.PushBack(g1, a);
      }

      auto json_stream = [&a, array_size](string source, string topic,
                                          string module) -> Value {
        Value g1;
        g1.SetObject();
        g1.AddMember("type", "group", a);
        g1.AddMember("name", Value(source.c_str(), a), a);
        Value attr;
        attr.SetObject();
        attr.AddMember("NX_class", "NXinstrument", a);
        g1.AddMember("attributes", attr, a);
        Value ch;
        ch.SetArray();
        {
          auto &children = ch;
          Value ds1;
          ds1.SetObject();
          ds1.AddMember("type", "stream", a);
          Value attr;
          attr.SetObject();
          attr.AddMember("this_will_be_a_double", Value(0.123), a);
          attr.AddMember("this_will_be_a_int64", Value(123), a);
          ds1.AddMember("attributes", attr, a);
          Document cfg_nexus;
          cfg_nexus.Parse(R""(
            {
              "nexus": {
                "indices": {
                  "index_every_mb": 1
                },
                "chunk": {
                  "chunk_kb": 1024
                }
              }
            }
          )"");
          Value stream;
          stream.CopyFrom(cfg_nexus, a);
          stream.AddMember("topic", Value(topic.c_str(), a), a);
          stream.AddMember("source", Value(source.c_str(), a), a);
          stream.AddMember("writer_module", Value(module.c_str(), a), a);
          if (array_size == 0) {
            stream.AddMember("type", Value("double", a), a);
          } else {
            stream.AddMember("type", Value("float", a), a);
            stream.AddMember("array_size", Value().SetInt(array_size), a);
          }
          ds1.AddMember("stream", stream, a);
          children.PushBack(ds1, a);
        }
        g1.AddMember("children", ch, a);
        return g1;
      };

      for (auto &source : sources) {
        children.PushBack(json_stream(source.source, source.topic, "f142"), a);
      }
      {
        Document d;
        d.Parse(
            R"({"type":"group", "name":"a-subgroup", "children":[{"type":"group","name":"another-subgroup"}]})");
        children.PushBack(Value().CopyFrom(d, a), a);
      }
      nexus_structure.AddMember("children", children, a);
      j.AddMember("nexus_structure", nexus_structure, a);
      {
        Value v;
        v.SetObject();
        v.AddMember("file_name", StringRef("tmp-f142.h5"), a);
        j.AddMember("file_attributes", v, a);
      }
      j.AddMember("cmd", StringRef("FileWriter_new"), a);
      j.AddMember("job_id", StringRef("0000000data_f142"), a);
    }

    auto cmd = json_to_string(json_command);
    // LOG(Sev::Debug, "command: {}", cmd);

    auto &d = json_command;
    auto fname = get_string(&d, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), 8u);

    FileWriter::CommandHandler ch(main_opt, nullptr);

    int const feed_msgs_times = 1;
    std::mt19937 rnd_nn;

    if (feed_msgs_times > 1) {
      LOG(Sev::Error, "Sorry, can feed messages currently only once");
      exit(1);
    }

    for (int file_i = 0; file_i < 1; ++file_i) {
      unlink(string(fname).c_str());

      ch.handle(FileWriter::Msg::owned((char const *)cmd.data(), cmd.size()));
      ASSERT_EQ(ch.FileWriterTasks.size(), (size_t)1);

      auto &fwt = ch.FileWriterTasks.at(0);
      ASSERT_EQ(fwt->demuxers().size(), (size_t)1);

      LOG(Sev::Debug, "processing...");
      using CLK = std::chrono::steady_clock;
      using MS = std::chrono::milliseconds;
      auto t1 = CLK::now();
      for (auto &source : sources) {
        for (int i_feed = 0; i_feed < feed_msgs_times; ++i_feed) {
          LOG(Sev::Info, "feed {}", i_feed);
          for (auto &msg : source.msgs) {
            if (false) {
              auto v = binary_to_hex(msg.data(), msg.size());
              LOG(Sev::Debug, "msg:\n{:.{}}", v.data(), v.size());
            }
            fwt->demuxers().at(0).process_message(
                FileWriter::Msg::cheap(msg, main_opt.jm));
            source.n_fed++;
          }
        }
      }
      auto t2 = CLK::now();
      LOG(Sev::Debug, "processing done in {} ms",
          duration_cast<MS>(t2 - t1).count());
      LOG(Sev::Debug, "finishing...");
      send_stop(ch, json_command);
      ASSERT_EQ(ch.FileWriterTasks.size(), (size_t)0);
      auto t3 = CLK::now();
      LOG(Sev::Debug, "finishing done in {} ms",
          duration_cast<MS>(t3 - t2).count());
      LOG(Sev::Debug, "done in total {} ms",
          duration_cast<MS>(t3 - t1).count());
    }
  }

  static void attribute_string_scalar() {
    hdf5::property::FileAccessList fapl;
    //    fapl.driver(hdf5::file::MemoryDriver());

    FileWriter::HDFFile hdf_file;
    hdf_file.h5file = hdf5::file::create(
        "tmp-attr-scalar.h5", hdf5::file::AccessFlags::TRUNCATE,
        hdf5::property::FileCreationList(), fapl);

    rapidjson::Document nexus_structure;
    nexus_structure.Parse(R""({
      "children": [
        {
          "type": "group",
          "name": "group1",
          "attributes": {
            "hello": "world"
          }
        }
      ]
    })"");
    ASSERT_EQ(nexus_structure.HasParseError(), false);
    std::vector<FileWriter::StreamHDFInfo> stream_hdf_info;
    hdf_file.init(nexus_structure, stream_hdf_info);

    auto a1 = hdf5::node::get_group(hdf_file.root_group, "/group1")
                  .attributes["hello"];
    ASSERT_EQ(a1.datatype().get_class(), hdf5::datatype::Class::STRING);
    std::string val;
    a1.read(val, a1.datatype());
    ASSERT_EQ(val, "world");
  }

  /// Read a string from the given dataset at the given position.
  /// Helper for other unit tests.
  /// So far only for 1d datasets.
  static std::string read_string(const hdf5::node::Dataset &dataset,
                                 std::vector<hsize_t> pos) {

    hdf5::datatype::String datatype(dataset.datatype());
    hdf5::dataspace::Simple dataspace(dataset.dataspace());

    std::vector<std::string> result;
    result.resize(dataspace.size());
    dataset.read(result, datatype, dataspace, dataspace,
                 hdf5::property::DatasetTransferList());

    // trim padding
    return result[pos[0]].c_str();
  }

  static void dataset_static_1d_string_fixed() {
    hdf5::property::FileAccessList fapl;
    //    fapl.driver(hdf5::file::MemoryDriver());

    FileWriter::HDFFile hdf_file;
    hdf_file.h5file =
        hdf5::file::create("tmp-fixedlen.h5", hdf5::file::AccessFlags::TRUNCATE,
                           hdf5::property::FileCreationList(), fapl);

    rapidjson::Document nexus_structure;
    nexus_structure.Parse(R""({
      "children": [
        {
          "type": "dataset",
          "name": "string_fixed_1d_fixed",
          "dataset": {
            "type":"string",
            "string_size": 71,
            "size": ["unlimited"]
          },
          "values": ["the-scalar-string", "another-one", "yet-another"]
        }
      ]
    })"");
    ASSERT_EQ(nexus_structure.HasParseError(), false);
    std::vector<FileWriter::StreamHDFInfo> stream_hdf_info;
    hdf_file.init(nexus_structure, stream_hdf_info);

    auto ds =
        hdf5::node::get_dataset(hdf_file.root_group, "string_fixed_1d_fixed");
    auto datatype = hdf5::datatype::String(ds.datatype());
    ASSERT_EQ(datatype.encoding(), hdf5::datatype::CharacterEncoding::UTF8);
    ASSERT_EQ(datatype.padding(), hdf5::datatype::StringPad::NULLTERM);
    ASSERT_FALSE(datatype.is_variable_length());
    ASSERT_EQ(read_string(ds, {1}), std::string("another-one"));
  }

  static void dataset_static_1d_string_variable() {
    hdf5::property::FileAccessList fapl;
    //    fapl.driver(hdf5::file::MemoryDriver());

    FileWriter::HDFFile hdf_file;
    hdf_file.h5file =
        hdf5::file::create("tmp-varlen.h5", hdf5::file::AccessFlags::TRUNCATE,
                           hdf5::property::FileCreationList(), fapl);

    rapidjson::Document nexus_structure;
    nexus_structure.Parse(R""({
      "children": [
        {
          "type": "dataset",
          "name": "string_fixed_1d_variable",
          "dataset": {
            "type":"string",
            "size": [3]
          },
          "values": ["string-0", "string-1", "string-2"]
        }
      ]
    })"");
    ASSERT_EQ(nexus_structure.HasParseError(), false);
    std::vector<FileWriter::StreamHDFInfo> stream_hdf_info;
    hdf_file.init(nexus_structure, stream_hdf_info);

    auto ds = hdf5::node::get_dataset(hdf_file.root_group,
                                      "string_fixed_1d_variable");
    auto datatype = hdf5::datatype::String(ds.datatype());
    ASSERT_EQ(datatype.encoding(), hdf5::datatype::CharacterEncoding::UTF8);
    ASSERT_EQ(datatype.padding(), hdf5::datatype::StringPad::NULLTERM);
    ASSERT_TRUE(datatype.is_variable_length());
    ASSERT_EQ(read_string(ds, {2}), std::string("string-2"));
  }
};

TEST_F(T_CommandHandler, new_03) { T_CommandHandler::new_03(); }

TEST_F(T_CommandHandler, create_static_file_with_hdf_output_prefix) {
  T_CommandHandler::create_static_file_with_hdf_output_prefix();
}

TEST_F(T_CommandHandler, create_static_dataset) {
  T_CommandHandler::create_static_dataset();
}

TEST_F(T_CommandHandler, write_attributes_at_top_level_of_the_file) {
  T_CommandHandler::write_attributes_at_top_level_of_the_file();
}

TEST_F(T_CommandHandler, data_ev42) { T_CommandHandler::data_ev42(); }

TEST_F(T_CommandHandler, data_f142) { T_CommandHandler::data_f142(); }

TEST_F(T_CommandHandler, attribute_string_scalar) {
  T_CommandHandler::attribute_string_scalar();
}

TEST_F(T_CommandHandler, dataset_static_1d_string_fixed) {
  T_CommandHandler::dataset_static_1d_string_fixed();
}

TEST_F(T_CommandHandler, dataset_static_1d_string_variable) {
  T_CommandHandler::dataset_static_1d_string_variable();
}
