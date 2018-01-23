#include "../HDFFile.h"
#include "../CommandHandler.h"
#include "../HDFFile_impl.h"
#include "../KafkaW.h"
#include "../MainOpt.h"
#include "../h5.h"
#include "../helper.h"
#include "../schemas/ev42/ev42_synth.h"
#include "../schemas/f142/f142_synth.h"
#include <array>
#include <chrono>
#include <gtest/gtest.h>
#include <random>
#include <rapidjson/document.h>
#include <string>
#include <unistd.h>
#include <vector>

using std::string;
using std::vector;
using std::array;
using std::chrono::steady_clock;
using std::chrono::milliseconds;
using std::chrono::duration_cast;

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
  ch.handle({(char *)cmd.data(), cmd.size()});
}

// Verify
TEST(HDFFile, create) {
  auto fname = "tmp-test.h5";
  unlink(fname);
  using namespace FileWriter;
  HDFFile f1;
  std::vector<StreamHDFInfo> stream_hdf_info;
  f1.init("tmp-test.h5", rapidjson::Value().SetObject(), stream_hdf_info);
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
    ch.handle({cmd.data(), cmd.size()});
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
    std::string hdf_output_prefix = "tmp-relative-output";
#ifdef _MSC_VER
#else
    mkdir(hdf_output_prefix.c_str(), 0777);
#endif
    {
      std::string jsontxt =
          fmt::format(R""({{"hdf-output-prefix": "{}"}})"", hdf_output_prefix);
      merge_config_into_main_opt(main_opt, jsontxt);
      main_opt.hdf_output_prefix = hdf_output_prefix;
    }
    rapidjson::Document json_command =
        basic_command("tmp-file-with-hdf-prefix.h5");
    command_add_static_dataset_1d(json_command);

    auto cmd = json_to_string(json_command);
    auto fname = get_string(&json_command, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), 8);

    FileWriter::CommandHandler ch(main_opt, nullptr);
    ch.handle({(char *)cmd.data(), cmd.size()});
    ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)1);
    send_stop(ch, json_command);
    ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)0);
    main_opt.hdf_output_prefix = "";

    // Verification
    auto fid = H5Fopen((hdf_output_prefix + "/" + fname.v).c_str(),
                       H5F_ACC_RDONLY, H5P_DEFAULT);
    ASSERT_GE(fid, 0);
    H5Fclose(fid);
  }

  static void create_static_dataset() {
    MainOpt &main_opt = *g_main_opt.load();
    merge_config_into_main_opt(main_opt, R""({})"");
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
        v.AddMember("file_name", StringRef("tmp-static-dataset.h5"), a);
        j.AddMember("file_attributes", v, a);
      }
      j.AddMember("cmd", StringRef("FileWriter_new"), a);
      j.AddMember("job_id", StringRef("000000000dataset"), a);
    }

    auto cmd = json_to_string(json_command);
    auto fname = get_string(&json_command, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), 8);

    FileWriter::CommandHandler ch(main_opt, nullptr);
    FileWriter::Msg msg;
    msg.data = (char *)cmd.data();
    msg.size = cmd.size();
    ch.handle(msg);
    ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)1);
    send_stop(ch, json_command);
    ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)0);

    // Verification
    auto fid = H5Fopen(string(fname).c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    ASSERT_GE(fid, 0);
    auto g1 = H5Gopen2(fid, "some_group", H5P_DEFAULT);
    ASSERT_GE(g1, 0);
    auto ds = H5Dopen2(g1, "value", H5P_DEFAULT);
    ASSERT_GE(ds, 0);
    ASSERT_GT(H5Tequal(H5Dget_type(ds), H5T_NATIVE_DOUBLE), 0);
    auto attr = H5Aopen(ds, "units", H5P_DEFAULT);
    ASSERT_GE(attr, 0);
    H5Aclose(attr);
    H5Dclose(ds);
    H5Gclose(g1);
    H5Fclose(fid);
  }

  static void write_attributes_at_top_level_of_the_file() {
    MainOpt &main_opt = *g_main_opt.load();
    merge_config_into_main_opt(main_opt, R""({})"");
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
        "file_name": "tmp_write_top_level_attributes.h5"
      },
      "job_id": "832yhtwgfskdf"
    })"");
    auto cmd = json_to_string(json_command);
    auto fname = get_string(&json_command, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), 8);

    FileWriter::CommandHandler ch(main_opt, nullptr);
    FileWriter::Msg msg;
    msg.data = (char *)cmd.data();
    msg.size = cmd.size();
    ch.handle(msg);
    ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)1);
    {
      string cmd(R""({
        "recv_type": "FileWriter",
        "cmd": "file_writer_tasks_clear_all",
        "job_id": "832yhtwgfskdf"
      })"");
      ch.handle({(char *)cmd.data(), cmd.size()});
    }

    // Verification
    auto fid = H5Fopen(string(fname).c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    ASSERT_GE(fid, 0);
    {
      auto attr = H5Aopen(fid, "some_top_level_int", H5P_DEFAULT);
      ASSERT_GE(attr, 0);
      auto dtype = H5Aget_type(attr);
      ASSERT_EQ(H5Tget_class(dtype), H5T_INTEGER);
      uint32_t v = 0;
      H5Aread(attr, H5T_NATIVE_INT32, &v);
      ASSERT_EQ(v, 42);
      H5Aclose(attr);
    }
    {
      auto attr = H5Aopen(fid, "some_top_level_string", H5P_DEFAULT);
      ASSERT_GE(attr, 0);
      auto dtype = H5Aget_type(attr);
      ASSERT_EQ(H5Tget_class(dtype), H5T_STRING);
      std::vector<char> buf;
      buf.resize(128);
      H5Aread(attr, dtype, buf.data());
      buf[buf.size() - 1] = 0;
      ASSERT_EQ(string("Hello Attribute"), buf.data());
      H5Aclose(attr);
    }
    {
      auto attr = H5Aopen(fid, "HDF5_Version", H5P_DEFAULT);
      ASSERT_GE(attr, 0);
      auto dtype = H5Aget_type(attr);
      ASSERT_EQ(H5Tget_class(dtype), H5T_STRING);
      auto dtype_mem = H5Tget_native_type(dtype, H5T_DIR_ASCEND);
      std::vector<char> buf;
      buf.resize(128);
      H5Aread(attr, dtype_mem, buf.data());
      buf[buf.size() - 1] = 0;
      ASSERT_EQ(FileWriter::h5_version_string_linked(), buf.data());
      H5Aclose(attr);
    }
    H5Fclose(fid);
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
    /// Generates n test messages which we can later feed from memory into the
    /// file writer.
    void pregenerate(int n, int n_events_per_message) {
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
        msgs.push_back(FileWriter::Msg{(char *)fb.builder->GetBufferPointer(),
                                       fb.builder->GetSize()});
      }
    }
  };

  static int recreate_file(rapidjson::Value *json_command) {
    // now try to recreate the file for testing:
    auto m = json_command->FindMember("file_attributes");
    auto fn = m->value.GetObject().FindMember("file_name")->value.GetString();
    auto x = H5Fcreate(fn, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    if (x < 0) {
      return -1;
    }
    H5Fclose(x);
    return 0;
  }

  /// Used by `data_ev42` test to verify attributes attached to the group.
  static void verify_attribute_data_ev42(hid_t oid, string const &group_path) {
    herr_t err;
    auto a1 = H5Aopen_by_name(oid, group_path.data(), "this_will_be_a_double",
                              H5P_DEFAULT, H5P_DEFAULT);
    ASSERT_GE(a1, 0);
    {
      auto dt = H5Aget_type(a1);
      ASSERT_EQ(H5Tget_class(dt), H5T_FLOAT);
      ASSERT_EQ(H5Tget_size(dt), H5Tget_size(H5T_NATIVE_DOUBLE));
      err = H5Tclose(dt);
      ASSERT_GE(err, 0);
    }
    double v;
    err = H5Aread(a1, H5T_NATIVE_DOUBLE, &v);
    ASSERT_EQ(v, 0.125);
    err = H5Aclose(a1);
    ASSERT_GE(err, 0);
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
          "chunk_n_elements": 64
        }
      },
      "unit_test": {
        "n_events_per_message": 32,
        "n_msgs_per_source": 128,
        "n_sources": 8,
        "n_msgs_per_batch": 1
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
      n_msgs_per_source = x.v;
    }

    int n_sources = 1;
    if (auto x = get_int(&main_opt.config_file, "unit_test.n_sources")) {
      LOG(Sev::Debug, "unit_test.n_sources: {}", x.v);
      n_sources = x.v;
    }

    int n_events_per_message = 1;
    if (auto x =
            get_int(&main_opt.config_file, "unit_test.n_events_per_message")) {
      LOG(Sev::Debug, "unit_test.n_events_per_message: {}", x.v);
      n_events_per_message = x.v;
    }

    int n_msgs_per_batch = 1;
    if (auto x = get_int(&main_opt.config_file, "unit_test.n_msgs_per_batch")) {
      LOG(Sev::Debug, "unit_test.n_msgs_per_batch: {}", x.v);
      n_msgs_per_batch = x.v;
    }

    vector<SourceDataGen> sources;
    for (int i1 = 0; i1 < n_sources; ++i1) {
      sources.emplace_back();
      auto &s = sources.back();
      // Currently, we assume only one topic!
      s.topic = "topic.with.multiple.sources";
      s.source = fmt::format("for_example_motor_{:04}", i1);
      s.pregenerate(n_msgs_per_source, n_events_per_message);
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

      auto json_stream = [&a](string source, string topic,
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
          Document ds1(&a);
          ds1.SetObject();
          ds1.Parse(R""({
            "type": "stream",
            "attributes": {
              "this_will_be_a_double": 0.125,
              "this_will_be_a_int64": 123
            }
          })"");
          Document stream(&a);
          stream.Parse(R""(
            {
              "nexus": {
                "indices": {
                  "index_every_kb": 1
                },
                "chunk": {
                  "chunk_n_elements": 64
                }
              }
            }
          )"");
          stream.AddMember("topic", Value(topic.c_str(), a), a);
          stream.AddMember("source", Value(source.c_str(), a), a);
          stream.AddMember("writer_module", Value(module.c_str(), a), a);
          stream.AddMember("type", Value("uint32", a), a);
          ds1.AddMember("stream", stream, a);
          children.PushBack(ds1, a);
        }
        g1.AddMember("children", ch, a);
        return g1;
      };

      for (auto &source : sources) {
        children.PushBack(json_stream(source.source, source.topic, "ev42"), a);
      }
      nexus_structure.AddMember("children", children, a);
      j.AddMember("nexus_structure", nexus_structure, a);
      {
        Value v;
        v.SetObject();
        v.AddMember("file_name", StringRef("tmp-ev42.h5"), a);
        j.AddMember("file_attributes", v, a);
      }
      j.AddMember("cmd", StringRef("FileWriter_new"), a);
      j.AddMember("job_id", StringRef("000000000042"), a);
    }

    auto cmd = json_to_string(json_command);
    // LOG(Sev::Dbg, "command: {}", cmd);

    auto &d = json_command;
    auto fname = get_string(&d, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), size_t{8});

    FileWriter::CommandHandler ch(main_opt, nullptr);

    using DT = uint32_t;
    int const feed_msgs_times = 1;
    std::mt19937 rnd_nn;

    for (int file_i = 0; file_i < 1; ++file_i) {
      unlink(string(fname).c_str());

      FileWriter::Msg msg;
      msg.data = (char *)cmd.data();
      msg.size = cmd.size();
      ch.handle(msg);
      ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)1);

      auto &fwt = ch.file_writer_tasks.at(0);
      ASSERT_EQ(fwt->demuxers().size(), (size_t)1);

      LOG(Sev::Debug, "processing...");
      using CLK = std::chrono::steady_clock;
      using MS = std::chrono::milliseconds;
      auto t1 = CLK::now();
      for (;;) {
        bool change = false;
        for (int i1 = 0; i1 < feed_msgs_times; ++i1) {
          for (auto &source : sources) {
            for (int i2 = 0;
                 i2 < n_msgs_per_batch && source.n_fed < source.msgs.size();
                 ++i2) {
              auto &msg = source.msgs[source.n_fed];
              if (false) {
                auto v = binary_to_hex(msg.data, msg.size);
                LOG(Sev::Debug, "msg:\n{:.{}}", v.data(), v.size());
              }
              fwt->demuxers().at(0).process_message(msg.data, msg.size);
              source.n_fed++;
              change = true;
            }
          }
        }
        if (!change) {
          break;
        }
      }
      auto t2 = CLK::now();
      LOG(Sev::Debug, "processing done in {} ms",
          duration_cast<MS>(t2 - t1).count());
      LOG(Sev::Debug, "finishing...");
      send_stop(ch, json_command);
      ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)0);
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

    herr_t err;

    auto fid = H5Fopen(string(fname).c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    ASSERT_GE(fid, 0);

    vector<DT> data((size_t)32000);

    for (auto &source : sources) {
      string base_path = "/";
      string group_path = base_path + source.source;

      auto g0 = H5Gopen(fid, group_path.c_str(), H5P_DEFAULT);
      ASSERT_GE(g0, 0);
      H5Gclose(g0);

      auto ds = H5Dopen2(fid, (group_path + "/event_id").c_str(), H5P_DEFAULT);
      ASSERT_GE(ds, 0);
      ASSERT_GT(H5Iis_valid(ds), 0);

      auto dt = H5Dget_type(ds);
      ASSERT_GT(dt, 0);
      ASSERT_EQ(H5Tget_size(dt), sizeof(DT));

      auto dsp = H5Dget_space(ds);
      ASSERT_GT(dsp, 0);
      ASSERT_EQ(H5Sis_simple(dsp), 1);

      using A = array<hsize_t, 1>;
      A sini = {{(hsize_t)n_events_per_message}};
      A smax = {{(hsize_t)n_events_per_message}};
      A count = {{(hsize_t)n_events_per_message}};
      A start0 = {{(hsize_t)0}};
      auto mem = H5Screate(H5S_SIMPLE);
      err = H5Sset_extent_simple(mem, sini.size(), sini.data(), smax.data());
      ASSERT_GE(err, 0);
      err = H5Sselect_hyperslab(mem, H5S_SELECT_SET, start0.data(), nullptr,
                                count.data(), nullptr);
      ASSERT_GE(err, 0);

      // LOG(Sev::Dbg, "have {} messages", source.msgs.size());
      for (size_t msg_i = 0; msg_i < source.msgs.size(); ++msg_i) {
        auto &fb = source.fbs.at(msg_i);
        A start = {{(hsize_t)msg_i * n_events_per_message}};
        err = H5Sselect_hyperslab(dsp, H5S_SELECT_SET, start.data(), nullptr,
                                  count.data(), nullptr);
        ASSERT_GE(err, 0);
        err =
            H5Dread(ds, h5::nat_type<DT>(), mem, dsp, H5P_DEFAULT, data.data());
        ASSERT_GE(err, 0);
        auto fbd = fb.root()->detector_id();
        for (int i1 = 0; i1 < n_events_per_message; ++i1) {
          // LOG(Sev::Dbg, "found: {:4}  {:6} vs {:6}", i1, data.at(i1),
          ASSERT_EQ(data.at(i1), fbd->Get(i1));
        }
      }

      H5Sclose(mem);
      H5Sclose(dsp);

      auto ds_cue_timestamp_zero = H5Dopen2(
          fid, (group_path + "/cue_timestamp_zero").c_str(), H5P_DEFAULT);
      ASSERT_GE(ds_cue_timestamp_zero, 0);
      ASSERT_GT(H5Iis_valid(ds_cue_timestamp_zero), 0);
      vector<uint64_t> cue_timestamp_zero(
          H5Sget_simple_extent_npoints(H5Dget_space(ds_cue_timestamp_zero)));
      err = H5Dread(ds_cue_timestamp_zero, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL,
                    H5P_DEFAULT, cue_timestamp_zero.data());
      ASSERT_EQ(err, 0);
      H5Dclose(ds_cue_timestamp_zero);

      auto ds_cue_index =
          H5Dopen2(fid, (group_path + "/cue_index").c_str(), H5P_DEFAULT);
      ASSERT_GE(ds_cue_index, 0);
      ASSERT_GT(H5Iis_valid(ds_cue_index), 0);
      vector<uint32_t> cue_index(
          H5Sget_simple_extent_npoints(H5Dget_space(ds_cue_index)));
      err = H5Dread(ds_cue_index, H5T_NATIVE_UINT32, H5S_ALL, H5S_ALL,
                    H5P_DEFAULT, cue_index.data());
      ASSERT_EQ(err, 0);
      H5Dclose(ds_cue_index);

      ASSERT_GE(cue_timestamp_zero.size(),
                minimum_expected_entries_in_the_index);
      ASSERT_EQ(cue_timestamp_zero.size(), cue_index.size());

      auto ds_event_time_zero =
          H5Dopen2(fid, (group_path + "/event_time_zero").c_str(), H5P_DEFAULT);
      ASSERT_GE(ds_event_time_zero, 0);
      ASSERT_GT(H5Iis_valid(ds_event_time_zero), 0);
      vector<uint64_t> event_time_zero(
          H5Sget_simple_extent_npoints(H5Dget_space(ds_event_time_zero)));
      err = H5Dread(ds_event_time_zero, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL,
                    H5P_DEFAULT, event_time_zero.data());
      ASSERT_EQ(err, 0);
      H5Dclose(ds_event_time_zero);

      auto ds_event_index =
          H5Dopen2(fid, (group_path + "/event_index").c_str(), H5P_DEFAULT);
      ASSERT_GE(ds_event_index, 0);
      ASSERT_GT(H5Iis_valid(ds_event_index), 0);
      vector<uint32_t> event_index(
          H5Sget_simple_extent_npoints(H5Dget_space(ds_event_index)));
      err = H5Dread(ds_event_index, H5T_NATIVE_UINT32, H5S_ALL, H5S_ALL,
                    H5P_DEFAULT, event_index.data());
      ASSERT_EQ(err, 0);
      H5Dclose(ds_event_index);

      ASSERT_GT(event_time_zero.size(), 0u);
      ASSERT_EQ(event_time_zero.size(), event_index.size());

      for (hsize_t i1 = 0; i1 < cue_timestamp_zero.size(); ++i1) {
        auto ok = check_cue(event_time_zero, event_index,
                            cue_timestamp_zero[i1], cue_index[i1]);
        ASSERT_TRUE(ok);
      }

      H5Tclose(dt);
      H5Dclose(ds);

      verify_attribute_data_ev42(fid, group_path);
    }

    H5Fclose(fid);

    ASSERT_EQ(recreate_file(&json_command), 0);
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
    void pregenerate(size_t array_size, uint64_t n) {
      LOG(Sev::Debug, "generating {} {}...", topic, source);
      auto ty = FlatBufs::f142::Value::Double;
      if (array_size > 0) {
        ty = FlatBufs::f142::Value::ArrayFloat;
      }
      FlatBufs::f142::synth synth(source, ty, int(array_size));
      rnd.seed(seed);
      for (uint64_t i1 = 0; i1 < n; ++i1) {
        // Number of events per message:
        // size_t n_ele = rnd() >> 24;
        // Currently fixed, have to adapt verification code first.
        fbs.push_back(synth.next(i1));
        auto &fb = fbs.back();
        msgs.push_back(FileWriter::Msg{(char *)fb.builder->GetBufferPointer(),
                                       fb.builder->GetSize()});
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
          "chunk_n_elements": 64
        }
      },
      "unit_test": {
        "n_events_per_message": 32,
        "n_msgs_per_source": 128,
        "n_sources": 1,
        "n_msgs_per_batch": 1
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

    int n_msgs_per_batch = 1;
    if (auto x = get_int(&main_opt.config_file, "unit_test.n_msgs_per_batch")) {
      LOG(Sev::Debug, "unit_test.n_msgs_per_batch: {}", x.v);
      n_msgs_per_batch = int(x.v);
    }

    size_t array_size = 4;
    vector<SourceDataGen_f142> sources;
    for (int i1 = 0; i1 < n_sources; ++i1) {
      sources.emplace_back();
      auto &s = sources.back();
      // Currently, we assume only one topic!
      s.topic = "topic.with.multiple.sources";
      s.source = fmt::format("for_example_motor_{:04}", i1);
      s.pregenerate(array_size, n_msgs_per_source);
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
                  "chunk_n_elements": 64
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
    // LOG(Sev::Dbg, "command: {}", cmd);

    auto &d = json_command;
    auto fname = get_string(&d, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), 8);

    FileWriter::CommandHandler ch(main_opt, nullptr);

    int const feed_msgs_times = 1;
    std::mt19937 rnd_nn;

    for (int file_i = 0; file_i < 1; ++file_i) {
      unlink(string(fname).c_str());

      FileWriter::Msg msg;
      msg.data = (char *)cmd.data();
      msg.size = cmd.size();
      ch.handle(msg);
      ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)1);

      auto &fwt = ch.file_writer_tasks.at(0);
      ASSERT_EQ(fwt->demuxers().size(), (size_t)1);

      LOG(Sev::Debug, "processing...");
      using CLK = std::chrono::steady_clock;
      using MS = std::chrono::milliseconds;
      auto t1 = CLK::now();
      for (;;) {
        bool change = false;
        for (int i1 = 0; i1 < feed_msgs_times; ++i1) {
          for (auto &source : sources) {
            for (int i2 = 0;
                 i2 < n_msgs_per_batch && source.n_fed < source.msgs.size();
                 ++i2) {
              auto &msg = source.msgs[source.n_fed];
              if (false) {
                auto v = binary_to_hex(msg.data, msg.size);
                LOG(Sev::Debug, "msg:\n{:.{}}", v.data(), v.size());
              }
              fwt->demuxers().at(0).process_message(msg.data, msg.size);
              source.n_fed++;
              change = true;
            }
          }
        }
        if (!change) {
          break;
        }
      }
      auto t2 = CLK::now();
      LOG(Sev::Debug, "processing done in {} ms",
          duration_cast<MS>(t2 - t1).count());
      LOG(Sev::Debug, "finishing...");
      send_stop(ch, json_command);
      ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)0);
      auto t3 = CLK::now();
      LOG(Sev::Debug, "finishing done in {} ms",
          duration_cast<MS>(t3 - t2).count());
      LOG(Sev::Debug, "done in total {} ms",
          duration_cast<MS>(t3 - t1).count());
    }
  }

  static void attribute_int_scalar() {
    auto fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_core(fapl, 1024 * 1024, false);
    auto h5file =
        H5Fcreate("tmp-in-memory.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
    H5Pclose(fapl);
    std::vector<FileWriter::StreamHDFInfo> stream_hdf_info;
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
    FileWriter::HDFFile hdf_file;
    hdf_file.h5file = h5file;
    hdf_file.init(h5file, "tmp-in-memory.h5", nexus_structure, stream_hdf_info);
    herr_t err;
    err = 0;
    auto a1 =
        H5Aopen_by_name(h5file, "/group1", "hello", H5P_DEFAULT, H5P_DEFAULT);
    ASSERT_GE(a1, 0);
    auto dt = H5Aget_type(a1);
    ASSERT_GE(dt, 0);
    ASSERT_EQ(H5Tget_class(dt), H5T_STRING);
    H5Tclose(dt);
    ASSERT_GE(H5Aclose(a1), 0);
  }

  /// Read a string from the given dataset at the given position.
  /// Helper for other unit tests.
  /// So far only for 1d datasets.
  static void read_string(std::string &result, hid_t ds,
                          std::vector<hsize_t> pos) {
    herr_t err;
    auto dt = H5Dget_type(ds);
    ASSERT_GE(dt, 0);
    ASSERT_EQ(H5Tget_class(dt), H5T_STRING);
    ASSERT_EQ(H5Tget_cset(dt), H5T_CSET_UTF8);
    if (!H5Tis_variable_str(dt)) {
      // Check plausibility, assuming current unit tests:
      ASSERT_LE(H5Tget_size(dt), 4096);
    }
    auto dsp = H5Dget_space(ds);
    ASSERT_GE(dsp, 0);
    {
      std::array<hsize_t, 1> now{{0}};
      std::array<hsize_t, 1> max{{0}};
      err = H5Sget_simple_extent_dims(dsp, now.data(), max.data());
      ASSERT_EQ(err, 1);
      ASSERT_GE(now.at(0), 0);
      ASSERT_GE(max.at(0), 0);
    }
    {
      std::array<hsize_t, 1> start{{pos.at(0)}};
      std::array<hsize_t, 1> stride{{1}};
      std::array<hsize_t, 1> count{{1}};
      std::array<hsize_t, 1> block{{1}};
      err = H5Sselect_hyperslab(dsp, H5S_SELECT_SET, start.data(),
                                stride.data(), count.data(), block.data());
      ASSERT_GE(err, 0);
    }
    auto dspmem = H5Screate(H5S_SIMPLE);
    ASSERT_GE(dspmem, 0);
    {
      std::array<hsize_t, 1> now{{1}};
      std::array<hsize_t, 1> max{{1}};
      err = H5Sset_extent_simple(dspmem, 1, now.data(), max.data());
      ASSERT_GE(err, 0);
    }
    {
      std::array<hsize_t, 1> start{{0}};
      std::array<hsize_t, 1> stride{{1}};
      std::array<hsize_t, 1> count{{1}};
      std::array<hsize_t, 1> block{{1}};
      err = H5Sselect_hyperslab(dspmem, H5S_SELECT_SET, start.data(),
                                stride.data(), count.data(), block.data());
      ASSERT_GE(err, 0);
    }
    auto dtmem = H5Tcopy(H5T_C_S1);
    H5Tset_cset(dtmem, H5T_CSET_UTF8);

    if (H5Tis_variable_str(dt)) {
      H5Tset_size(dtmem, H5T_VARIABLE);
      char *string_ptr = nullptr;
      err = H5Dread(ds, dtmem, dspmem, dsp, H5P_DEFAULT, &string_ptr);
      ASSERT_GE(err, 0);
      result = std::string(string_ptr);
    } else {
      H5Tset_size(dtmem, H5Tget_size(dt));
      std::vector<char> buf;
      buf.resize(H5Tget_size(dt) + 1);
      err = H5Dread(ds, dtmem, dspmem, dsp, H5P_DEFAULT, buf.data());
      ASSERT_GE(err, 0);
      result = std::string(buf.data());
    }

    H5Tclose(dt);
    ASSERT_GE(H5Sclose(dsp), 0);
    ASSERT_GE(H5Sclose(dspmem), 0);
  }

  static void dataset_static_1d_string_fixed() {
    auto fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_core(fapl, 1024 * 1024, false);
    auto h5file =
        H5Fcreate("tmp-in-memory.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
    H5Pclose(fapl);
    std::vector<FileWriter::StreamHDFInfo> stream_hdf_info;
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
          "values": ["the-scalar-string", "another-one"]
        }
      ]
    })"");
    ASSERT_EQ(nexus_structure.HasParseError(), false);
    FileWriter::HDFFile hdf_file;
    hdf_file.h5file = h5file;
    hdf_file.init(h5file, "tmp-in-memory.h5", nexus_structure, stream_hdf_info);
    herr_t err;
    err = 0;
    auto ds = H5Dopen(h5file, "/string_fixed_1d_fixed", H5P_DEFAULT);
    ASSERT_GE(ds, 0);
    std::string item;
    read_string(item, ds, {1});
    ASSERT_EQ(item, "another-one");
    ASSERT_GE(H5Dclose(ds), 0);
  }

  static void dataset_static_1d_string_variable() {
    auto fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_core(fapl, 1024 * 1024, false);
    auto h5file =
        H5Fcreate("tmp-in-memory.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
    H5Pclose(fapl);
    std::vector<FileWriter::StreamHDFInfo> stream_hdf_info;
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
    FileWriter::HDFFile hdf_file;
    hdf_file.h5file = h5file;
    hdf_file.init(h5file, "tmp-in-memory.h5", nexus_structure, stream_hdf_info);
    herr_t err;
    err = 0;
    auto ds = H5Dopen(h5file, "/string_fixed_1d_variable", H5P_DEFAULT);
    ASSERT_GE(ds, 0);
    std::string item;
    read_string(item, ds, {2});
    ASSERT_EQ(item, "string-2");
    ASSERT_GE(H5Dclose(ds), 0);
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

TEST_F(T_CommandHandler, attribute_int_scalar) {
  T_CommandHandler::attribute_int_scalar();
}

TEST_F(T_CommandHandler, dataset_static_1d_string_fixed) {
  T_CommandHandler::dataset_static_1d_string_fixed();
}

TEST_F(T_CommandHandler, dataset_static_1d_string_variable) {
  T_CommandHandler::dataset_static_1d_string_variable();
}
