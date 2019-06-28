#include "../CommandHandler.h"
#include "../HDFFile.h"
#include "../KafkaW/Consumer.h"
#include "../MainOpt.h"
#include "../helper.h"
#include "../json.h"
#include "AddReader.h"
#include "tests/helpers/HDFFileTestHelper.h"
#include "tests/helpers/ev42_synth.h"
#include "tests/helpers/f142_synth.h"
#include <array>
#include <chrono>
#include <gtest/gtest.h>
#include <hdf5.h>
#include <random>
#include <string>
#include <unistd.h>
#include <vector>

using std::string;
using std::vector;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using nlohmann::json;

MainOpt getTestOptions() {
  MainOpt TestOptions;
  TestOptions.init();
  return TestOptions;
}

void merge_config_into_main_opt(MainOpt &main_opt, string JSONString) {
  main_opt.CommandsJson.merge_patch(json::parse(JSONString));
}

json basic_command(string filename) {
  auto Command = json::parse(R""({
    "cmd": "FileWriter_new",
    "nexus_structure": {
      "children": []
    },
    "file_attributes": {
    },
    "job_id": "some_unique_id"
  })"");
  Command["file_attributes"]["file_name"] = filename;
  return Command;
}

void command_add_static_dataset_1d(json &Command) {
  auto Json = json::parse(R""({
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
  Command["nexus_structure"]["children"].push_back(Json);
}

void send_stop(FileWriter::CommandHandler &ch, json const &CommandJSON) {
  auto Command = json::parse(R""({
    "recv_type": "FileWriter",
    "cmd": "file_writer_tasks_clear_all"
  })"");
  Command["job_id"] = CommandJSON["job_id"];
  auto CommandString = Command.dump();
  ch.tryToHandle(CommandString);
}

// Verify
TEST(HDFFile, Create) {
  auto fname = "tmp-test.h5";
  unlink(fname);
  FileWriter::HDFFile f1;
  std::vector<FileWriter::StreamHDFInfo> stream_hdf_info;
  f1.init("tmp-test.h5", nlohmann::json::object(), nlohmann::json::object(),
          stream_hdf_info, true);
}

class T_CommandHandler : public testing::Test {
public:
  static void new_03() {
    auto StaticLogger = spdlog::get("testlogger");
    auto CommandData = readFileIntoVector(std::string(TEST_DATA_PATH) +
                                          "/msg-cmd-new-03.json");
    std::string CommandString(CommandData.data(),
                              CommandData.data() + CommandData.size());
    StaticLogger->trace("CommandString: {:.{}}", CommandString.data(),
                        CommandString.size());
    auto Command = json::parse(CommandString);
    std::string Filename = Command["file_attributes"]["file_name"];
    std::remove(Filename.c_str());
    MainOpt MainOptions;

    FileWriter::CommandHandler CommandHandlerToTest(MainOptions, nullptr);
    CommandHandlerToTest.tryToHandle(CommandString);
  }

  static void new_04() {
    auto StaticLogger = spdlog::get("testlogger");

    auto CommandData = readFileIntoVector(std::string(TEST_DATA_PATH) +
                                          "/msg-cmd-new-04.json");
    std::string CommandString(CommandData.data(),
                              CommandData.data() + CommandData.size());
    StaticLogger->trace("CommandString: {:.{}}", CommandString.data(),
                        CommandString.size());
    auto Command = json::parse(CommandString);
    std::string fname = Command["file_attributes"]["file_name"];
    unlink(fname.c_str());
    MainOpt main_opt;
    FileWriter::CommandHandler ch(main_opt, nullptr);
    ch.tryToHandle(CommandString);
  }

  static void create_static_file_with_hdf_output_prefix() {
    MainOpt main_opt = getTestOptions();
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
      main_opt.HDFOutputPrefix = hdf_output_prefix;
    }
    auto json_command = basic_command(hdf_output_filename);
    command_add_static_dataset_1d(json_command);

    auto Command = json_command.dump();
    std::string fname = json_command["file_attributes"]["file_name"];
    ASSERT_GT(fname.size(), 8u);

    FileWriter::CommandHandler ch(main_opt, nullptr);
    ch.tryToHandle(Command);
    ASSERT_EQ(ch.getNumberOfFileWriterTasks(), static_cast<size_t>(1));
    send_stop(ch, json_command);
    ASSERT_EQ(ch.getNumberOfFileWriterTasks(), static_cast<size_t>(0));
    main_opt.HDFOutputPrefix = "";

    // Verification
    auto file = hdf5::file::open(hdf_output_prefix + "/" + fname,
                                 hdf5::file::AccessFlags::READONLY);
    ASSERT_TRUE(file.is_valid());
  }

  static void create_static_dataset() {
    MainOpt main_opt = getTestOptions();
    merge_config_into_main_opt(main_opt, R""({})"");
    std::string const hdf_output_filename = "tmp-static-dataset.h5";
    unlink(hdf_output_filename.c_str());
    auto CommandJSON = json::object();
    {
      auto NexusStructure = json::object();
      auto Children = json::array();
      {
        auto Group = json::parse(R""({
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
            },
            {
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
            },
            {
              "type": "dataset",
              "name": "string_scalar",
              "dataset": {
                "type": "string"
              },
              "values": "the-scalar-string"
            },
            {
              "type": "dataset",
              "name": "string_1d",
              "dataset": {
                "type": "string",
                "size": ["unlimited"]
              },
              "values": ["the-scalar-string", "another-one"]
            },
            {
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
            },
            {
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
            }
          ]
        })"");
        {
          auto Dataset = json::parse(
              R""({"type":"dataset", "name": "big_set", "dataset": {"space":"simple", "type":"double", "size":["unlimited", 4, 2]}})"");
          auto Values = json::array();
          for (size_t i1 = 0; i1 < 7; ++i1) {
            auto V1 = json::array();
            for (size_t i2 = 0; i2 < 4; ++i2) {
              auto V2 = json::array();
              for (size_t i3 = 0; i3 < 2; ++i3) {
                V2.push_back(1000 * i1 + 10 * i2 + i3);
              }
              V1.push_back(V2);
            }
            Values.push_back(V1);
          }
          Dataset["values"] = Values;
          Group["children"].push_back(Dataset);
        }
        Children.push_back(Group);
      }
      NexusStructure["children"] = Children;
      CommandJSON["nexus_structure"] = NexusStructure;
      CommandJSON["file_attributes"] = json::object();
      CommandJSON["file_attributes"]["file_name"] = hdf_output_filename;
      CommandJSON["cmd"] = "FileWriter_new";
      CommandJSON["job_id"] = "000000000dataset";
    }

    auto CommandString = CommandJSON.dump();
    std::string Filename = CommandJSON["file_attributes"]["file_name"];
    ASSERT_GT(Filename.size(), 8u);

    FileWriter::CommandHandler ch(main_opt, nullptr);
    ch.tryToHandle(CommandString);
    ASSERT_EQ(ch.getNumberOfFileWriterTasks(), static_cast<size_t>(1));

    send_stop(ch, CommandJSON);
    ASSERT_EQ(ch.getNumberOfFileWriterTasks(), static_cast<size_t>(0));
    // Verification
    auto file = hdf5::file::open(Filename, hdf5::file::AccessFlags::READONLY);
    auto ds = hdf5::node::get_dataset(file.root(), "/some_group/value");
    ASSERT_EQ(ds.datatype(), hdf5::datatype::create<double>());
    ASSERT_TRUE(ds.attributes["units"].is_valid());
  }

  static void write_attributes_at_top_level_of_the_file() {
    MainOpt main_opt = getTestOptions();
    merge_config_into_main_opt(main_opt, R""({})"");
    std::string const hdf_output_filename = "tmp_write_top_level_attributes.h5";
    unlink(hdf_output_filename.c_str());
    auto CommandJSON = json::parse(R""({
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
    CommandJSON["file_attributes"]["file_name"] = hdf_output_filename;
    auto CommandString = CommandJSON.dump();
    std::string Filename = CommandJSON["file_attributes"]["file_name"];
    ASSERT_GT(Filename.size(), 8u);

    FileWriter::CommandHandler ch(main_opt, nullptr);
    ch.tryToHandle(CommandString);
    ASSERT_EQ(ch.getNumberOfFileWriterTasks(), static_cast<size_t>(1));
    send_stop(ch, CommandJSON);
    ASSERT_EQ(ch.getNumberOfFileWriterTasks(), static_cast<size_t>(0));

    // Verification
    auto file = hdf5::file::open(Filename, hdf5::file::AccessFlags::READONLY);
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
      ASSERT_EQ(FileWriter::h5VersionStringLinked(), val);
    }
    {
      auto attr = root_group.attributes["file_time"];
      ASSERT_EQ(attr.datatype().get_class(), hdf5::datatype::Class::STRING);
    }
  }

  /// \brief Can supply pre-generated test data for a source on a topic to
  /// profile
  /// the writing.
  class SourceDataGen {
  public:
    string topic;
    string source;
    uint64_t seed = 0;
    std::mt19937 rnd;
    vector<FlatBufs::ev42::FlatBufferWrapper> fbs;
    vector<FileWriter::Msg> msgs;
    // Number of messages already fed into file writer during testing
    size_t n_fed = 0;
    bool run_parallel = false;
    int n_events_per_message = 0;
    /// \brief Generates n test messages which can be later fed from memory into
    /// the
    /// file writer.
    void pregenerate(int n, int n_events_per_message_) {
      n_events_per_message = n_events_per_message_;
      Logger->trace("generating {} {}...", topic, source);
      FlatBufs::ev42::Synth synth(source, seed);
      rnd.seed(seed);
      for (int i1 = 0; i1 < n; ++i1) {
        // Number of events per message:
        // size_t n_ele = rnd() >> 24;
        // Currently fixed, have to adapt verification code first.
        auto n_ele = n_events_per_message;
        fbs.push_back(synth.next(n_ele));
        auto &fb = fbs.back();

        // Allocate memory on JM AND CHECK IT!
        msgs.push_back(FileWriter::Msg::owned(
            reinterpret_cast<const char *>(fb.Builder->GetBufferPointer()),
            fb.Builder->GetSize()));
        if (msgs.back().size() < 8) {
          Logger->error("error");
          exit(1);
        }
      }
    }

  private:
    SharedLogger Logger = spdlog::get("testlogger");
  };

  /// Used by `data_ev42` test to verify attributes attached to the group.
  static void verify_attribute_data_ev42(hdf5::node::Group &node) {

    auto a1 = node.attributes["this_will_be_a_double"];
    auto dt = a1.datatype();
    ASSERT_EQ(dt.get_class(), hdf5::datatype::Class::FLOAT);
    ASSERT_EQ(dt.size(), sizeof(double));
    double v{0};
    a1.read(v);
    ASSERT_EQ(v, 0.125);
  }

  static void data_ev42() {
    SharedLogger Logger = spdlog::get("filewriterlogger");
    AddEv42Reader();
    MainOpt main_opt = getTestOptions();

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

    bool do_verification = true;
    try {
      do_verification =
          main_opt.CommandsJson["unit_test"]["hdf"]["do_verification"]
              .get<int64_t>();
      Logger->trace("do_verification: {}", do_verification);
    } catch (...) {
    }

    size_t n_msgs_per_source = 1;
    try {
      n_msgs_per_source =
          main_opt.CommandsJson["unit_test"]["n_msgs_per_source"]
              .get<int64_t>();
      Logger->trace("unit_test.n_msgs_per_source: {}", n_msgs_per_source);
    } catch (...) {
    }

    size_t n_sources = 1;
    try {
      n_sources =
          main_opt.CommandsJson["unit_test"]["n_sources"].get<int64_t>();
      Logger->trace("unit_test.n_sources: {}", n_sources);
    } catch (...) {
    }

    size_t n_events_per_message = 1;
    try {
      n_events_per_message =
          main_opt.CommandsJson["unit_test"]["n_events_per_message"]
              .get<int64_t>();
      Logger->trace("unit_test.n_events_per_message: {}", n_events_per_message);
    } catch (...) {
    }

    size_t feed_msgs_times = 1;
    try {
      feed_msgs_times =
          main_opt.CommandsJson["unit_test"]["feed_msgs_times"].get<int64_t>();
      Logger->trace("unit_test.feed_msgs_times: {}", feed_msgs_times);
    } catch (...) {
    }

    int feed_msgs_seconds = 1;
    try {
      feed_msgs_seconds =
          main_opt.CommandsJson["unit_test"]["feed_msgs_seconds"]
              .get<int64_t>();
      Logger->trace("unit_test.feed_msgs_seconds: {}", feed_msgs_seconds);
    } catch (...) {
    }

    string filename = "tmp-ev42.h5";
    try {
      filename =
          main_opt.CommandsJson["unit_test"]["filename"].get<std::string>();
      Logger->trace("unit_test.filename: {}", filename);
    } catch (...) {
    }

    vector<SourceDataGen> sources;
    for (size_t i1 = 0; i1 < n_sources; ++i1) {
      sources.emplace_back();
      auto &s = sources.back();
      // Currently, we assume only one topic!
      s.topic = "topic.with.multiple.sources";
      s.source = fmt::format("for_example_motor_{:04}", i1);
      s.run_parallel = true;
      s.pregenerate(n_msgs_per_source, n_events_per_message);
    }

    sources.emplace_back();
    auto &s = sources.back();
    s.topic = "topic.with.multiple.sources";
    s.source = fmt::format("stream_for_main_thread_{:04}", 0);
    s.pregenerate(17, 71);

    auto CommandJSON = json::object();
    {
      auto NexusStructure = json::object();
      auto Children = json::array();
      {
        auto Group = json::parse(R""({
          "type": "group",
          "name": "some_group",
          "attributes": {
            "NX_class": "NXinstrument"
          },
          "children": []
        })"");
        Children.push_back(Group);
      }

      auto json_stream = [&main_opt](string Source, string Topic, string Module,
                                     bool run_parallel) -> json {
        auto Group = json::object();
        Group["type"] = "group";
        Group["name"] = Source;
        auto Attr = json::object();
        Attr["NX_class"] = "NXinstrument";
        Group["attributes"] = Attr;
        auto InnerChildren = json::array();
        {
          auto Dataset = json::parse(R""({
            "type": "stream",
            "attributes": {
              "this_will_be_a_double": 0.125,
              "this_will_be_a_int64": 123
            }
          })"");
          auto Stream = json::object();
          if (auto x = find<json>("nexus", main_opt.CommandsJson)) {
            Stream["nexus"] = x.inner();
          }
          Stream["topic"] = Topic;
          Stream["source"] = Source;
          Stream["writer_module"] = Module;
          Stream["type"] = "uint32";
          Stream["n_mpi_workers"] =
              main_opt.CommandsJson["unit_test"]["n_mpi_workers"]
                  .get<uint64_t>();
          Stream["run_parallel"] = run_parallel;
          Dataset["stream"] = Stream;
          InnerChildren.push_back(Dataset);
        }
        Group["children"] = InnerChildren;
        return Group;
      };

      for (auto &source : sources) {
        // cppcheck-suppress useStlAlgorithm
        Children.push_back(json_stream(source.source, source.topic, "ev42",
                                       source.run_parallel));
      }

      NexusStructure["children"] = Children;
      CommandJSON["nexus_structure"] = NexusStructure;
      CommandJSON["file_attributes"] = json::object();
      CommandJSON["file_attributes"]["file_name"] = filename;
      CommandJSON["cmd"] = "FileWriter_new";
      CommandJSON["job_id"] = "test-ev42";
    }

    Logger->trace("CommandJSON: {}", CommandJSON.dump());

    auto fname = CommandJSON["file_attributes"]["file_name"].get<std::string>();
    ASSERT_GT(fname.size(), size_t(8));

    FileWriter::CommandHandler ch(main_opt, nullptr);

    using DT = uint32_t;

    for (int file_i = 0; file_i < 1; ++file_i) {
      unlink(string(fname).c_str());

      auto CommandString = CommandJSON.dump();
      ch.tryToHandle(CommandString);
      ASSERT_EQ(ch.getNumberOfFileWriterTasks(), (size_t)1);

      auto &fwt = ch.getFileWriterTaskByJobID("test-ev42");
      ASSERT_EQ(fwt->demuxers().size(), static_cast<size_t>(1));

      Logger->trace("processing...");
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
            Logger->trace("i_feed: {:3}  i_source: {:2}", i_feed, i_source);
          }
          for (auto &msg : source.msgs) {
            if (msg.size() < 8) {
              Logger->error("error");
              do_run = false;
            }
            FileWriter::FlatbufferMessage TempMessage(msg.data(), msg.size());
            auto res =
                fwt->demuxers().at(0).process_message(std::move(TempMessage));
            if (res == FileWriter::ProcessMessageResult::ERR) {
              Logger->error("is_ERR");
              do_run = false;
              break;
            }
            if (res == FileWriter::ProcessMessageResult::ALL_SOURCES_FULL) {
              Logger->error("is_ALL_SOURCES_FULL");
              do_run = false;
              break;
            }
            if (res == FileWriter::ProcessMessageResult::STOP) {
              Logger->error("is_STOP");
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
      Logger->trace("processing done in {} ms",
                    duration_cast<MS>(t2 - t1).count());
      Logger->trace("finishing...");
      send_stop(ch, CommandJSON);
      ASSERT_EQ(ch.getNumberOfFileWriterTasks(), static_cast<size_t>(0));
      auto t3 = CLK::now();
      Logger->trace("finishing done in {} ms",
                    duration_cast<MS>(t3 - t2).count());
      Logger->trace("done in total {} ms", duration_cast<MS>(t3 - t1).count());
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
      vector<DT> data(static_cast<size_t>(source.n_events_per_message));
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

      ++i_source;

      auto attr_node = hdf5::node::get_group(root_group, group_path);
      verify_attribute_data_ev42(attr_node);
    }

    Logger->trace("data_ev42 verification done");
  }

  /// \brief Can supply pre-generated test data for a source on a topic to
  /// profile
  /// the writing.
  class SourceDataGen_f142 {
  public:
    SourceDataGen_f142() {}
    string topic;
    string source;
    uint64_t seed = 0;
    std::mt19937 rnd;
    vector<FlatBufs::f142::FlatBufferWrapper> fbs;
    vector<FileWriter::Msg> msgs;
    // Number of messages already fed into file writer during testing
    size_t n_fed = 0;
    /// \brief Generates n test messages which we can later feed from memory
    /// into the
    /// file writer.
    void pregenerate(size_t array_size, uint64_t n) {
      Logger->trace("generating {} {}...", topic, source);
      auto ty = FlatBufs::f142::Value::Double;
      if (array_size > 0) {
        ty = FlatBufs::f142::Value::ArrayFloat;
      }
      FlatBufs::f142::Synth synth(source, ty);
      rnd.seed(seed);
      for (uint64_t i1 = 0; i1 < n; ++i1) {
        // Number of events per message:
        // size_t n_ele = rnd() >> 24;
        // Currently fixed, have to adapt verification code first.
        fbs.push_back(synth.next(i1, array_size));
        auto &fb = fbs.back();
        msgs.push_back(FileWriter::Msg::owned(
            reinterpret_cast<const char *>(fb.builder->GetBufferPointer()),
            fb.builder->GetSize()));
      }
    }

  private:
    SharedLogger Logger = spdlog::get("testlogger");
  };

  static void data_f142() {
    SharedLogger Logger = spdlog::get("testlogger");
    AddF142Reader();
    MainOpt main_opt = getTestOptions();
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

    try {
      auto do_verification =
          main_opt.CommandsJson["unit_test"]["hdf"]["do_verification"]
              .get<uint64_t>();
      Logger->trace("do_verification: {}", do_verification);
    } catch (...) {
    }

    int n_msgs_per_source = 1;
    try {
      n_msgs_per_source =
          main_opt.CommandsJson["unit_test"]["n_msgs_per_source"]
              .get<uint64_t>();
      Logger->trace("n_msgs_per_source: {}", n_msgs_per_source);
    } catch (...) {
    }

    int n_sources = 1;
    try {
      n_sources =
          main_opt.CommandsJson["unit_test"]["n_sources"].get<uint64_t>();
      Logger->trace("n_sources: {}", n_sources);
    } catch (...) {
    }

    size_t array_size = 0;
    try {
      array_size =
          main_opt.CommandsJson["unit_test"]["array_size"].get<uint64_t>();
      Logger->trace("array_size: {}", array_size);
    } catch (...) {
    }

    vector<SourceDataGen_f142> sources;
    for (int i1 = 0; i1 < n_sources; ++i1) {
      sources.emplace_back();
      auto &s = sources.back();
      // Currently, we assume only one topic!
      s.topic = "topic.with.multiple.sources";
      s.source = fmt::format("for_example_motor_{:04}", i1);
      s.pregenerate(array_size, n_msgs_per_source);
    }

    auto CommandJSON = json::parse(R""({
      "nexus_structure": {
        "children": [
          {
            "type": "group",
            "name": "some_group",
            "attributes": {
              "NX_class": "NXinstrument"
            },
            "children": [
              {
                "type": "dataset",
                "name": "created_by_filewriter",
                "attributes": {
                  "NX_class": "NXdetector",
                  "this_will_be_a_double": 0.123,
                  "this_will_be_a_int64": 123
                },
                "dataset": {
                  "space": "simple",
                  "type": "uint64",
                  "size": ["unlimited", 4, 2]
                }
              }
            ]
          }
        ]
      }
    })"");

    {
      auto MakeStreamJSON = [array_size](string Source, string Topic,
                                         string Module) -> json {
        auto Group = json::parse(R""({
          "type": "group",
          "attributes": {
            "NX_class": "NXinstrument"
          },
          "children": [
            {
              "type": "stream",
              "attributes": {
                "this_will_be_a_double": 0.123,
                "this_will_be_a_int64": 123
              },
              "stream": {
                "nexus": {
                  "indices": {
                    "index_every_mb": 1
                  },
                  "chunk": {
                    "chunk_kb": 1024
                  }
                }
              }
            }
          ]
        })"");
        Group["name"] = Source;
        auto &Stream = Group["children"][0]["stream"];
        Stream["topic"] = Topic;
        Stream["source"] = Source;
        Stream["writer_module"] = Module;
        if (array_size == 0) {
          Stream["type"] = "double";
        } else {
          Stream["type"] = "float";
          Stream["array_size"] = array_size;
        }
        return Group;
      };

      for (auto &source : sources) {
        CommandJSON["nexus_structure"]["children"].push_back(
            MakeStreamJSON(source.source, source.topic, "f142"));
      }
      CommandJSON["nexus_structure"]["children"].push_back(json::parse(
          R""({"type":"group", "name":"a-subgroup", "children":[{"type":"group","name":"another-subgroup"}]})""));
      CommandJSON["file_attributes"] = json::object();
      CommandJSON["file_attributes"]["file_name"] = "tmp-f142.h5";
      CommandJSON["cmd"] = "FileWriter_new";
      CommandJSON["job_id"] = "unit_test_job_data_f142";
    }

    auto CommandString = CommandJSON.dump();
    std::string Filename = CommandJSON["file_attributes"]["file_name"];
    ASSERT_GT(Filename.size(), 8u);

    FileWriter::CommandHandler ch(main_opt, nullptr);

    int const feed_msgs_times = 1;

    for (int file_i = 0; file_i < 1; ++file_i) {
      unlink(Filename.c_str());

      ch.tryToHandle(CommandString);
      ASSERT_EQ(ch.getNumberOfFileWriterTasks(), static_cast<size_t>(1));
      auto &fwt = ch.getFileWriterTaskByJobID("unit_test_job_data_f142");
      ASSERT_EQ(fwt->demuxers().size(), static_cast<size_t>(1));

      Logger->trace("processing...");
      using CLK = std::chrono::steady_clock;
      using MS = std::chrono::milliseconds;
      auto t1 = CLK::now();
      for (auto &source : sources) {
        for (int i_feed = 0; i_feed < feed_msgs_times; ++i_feed) {
          Logger->info("feed {}", i_feed);
          for (auto &msg : source.msgs) {
            FileWriter::FlatbufferMessage TempMessage(msg.data(), msg.size());
            fwt->demuxers().at(0).process_message(std::move(TempMessage));
            source.n_fed++;
          }
        }
      }
      auto t2 = CLK::now();
      Logger->trace("processing done in {} ms",
                    duration_cast<MS>(t2 - t1).count());
      Logger->trace("finishing...");
      send_stop(ch, CommandJSON);
      ASSERT_EQ(ch.getNumberOfFileWriterTasks(), static_cast<size_t>(0));
      auto t3 = CLK::now();
      Logger->trace("finishing done in {} ms",
                    duration_cast<MS>(t3 - t2).count());
      Logger->trace("done in total {} ms", duration_cast<MS>(t3 - t1).count());
    }
  }

  /// \brief Read a string from the given dataset at the given position.
  ///
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
    return result[pos[0]];
  }

  static void dataset_static_1d_string_variable() {
    auto File = HDFFileTestHelper::createInMemoryTestFile("tmp-varlen.h5");
    auto NexusStructure = json::parse(R""({
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
    std::vector<FileWriter::StreamHDFInfo> stream_hdf_info;
    File.init(NexusStructure, stream_hdf_info);
    auto ds =
        hdf5::node::get_dataset(File.RootGroup, "string_fixed_1d_variable");
    auto datatype = hdf5::datatype::String(ds.datatype());
    ASSERT_EQ(datatype.encoding(), hdf5::datatype::CharacterEncoding::UTF8);
    ASSERT_EQ(datatype.padding(), hdf5::datatype::StringPad::NULLTERM);
    ASSERT_TRUE(datatype.is_variable_length());
    ASSERT_EQ(read_string(ds, {2}), std::string("string-2"));
  }
};

TEST_F(T_CommandHandler, New03) { T_CommandHandler::new_03(); }

TEST_F(T_CommandHandler, New04) { EXPECT_NO_THROW(T_CommandHandler::new_04()); }

TEST_F(T_CommandHandler, CreateStaticFileWithHdfOutputPrefix) {
  T_CommandHandler::create_static_file_with_hdf_output_prefix();
}

TEST_F(T_CommandHandler, CreateStaticDataset) {
  T_CommandHandler::create_static_dataset();
}

TEST_F(T_CommandHandler, WriteAttributesAtTopLevelOfTheFile) {
  T_CommandHandler::write_attributes_at_top_level_of_the_file();
}

TEST_F(T_CommandHandler, DataEv42) { T_CommandHandler::data_ev42(); }

TEST_F(T_CommandHandler, DataF142) { T_CommandHandler::data_f142(); }

TEST_F(T_CommandHandler, DatasetStatic1DStringVariable) {
  T_CommandHandler::dataset_static_1d_string_variable();
}

template <typename T>
void verifyWrittenDatatype(FileWriter::HDFFile &TestFile,
                           const std::pair<std::string, T> &NameAndValue) {
  auto Dataset =
      hdf5::node::get_dataset(TestFile.RootGroup, "/" + NameAndValue.first);
  auto OutputValue = NameAndValue.second;
  Dataset.read(OutputValue);
  ASSERT_EQ(OutputValue, NameAndValue.second);
}

TEST_F(T_CommandHandler, createStaticDatasetOfEachIntType) {
  using HDFFileTestHelper::createCommandForDataset;

  auto TestFile =
      HDFFileTestHelper::createInMemoryTestFile("test-dataset-int-types.nxs");

  const std::pair<std::string, uint32_t> TestUint32 = {"uint32", 37381149};
  const std::pair<std::string, uint64_t> TestUint64 = {"uint64",
                                                       10138143369737381149U};
  const std::pair<std::string, int32_t> TestInt32 = {"int32", -7381149};
  const std::pair<std::string, int64_t> TestInt64 = {"int64",
                                                     -138143369737381149};

  std::stringstream CommandStream;
  CommandStream << R""({"children": [)"" << createCommandForDataset(TestUint32)
                << ",\n"
                << createCommandForDataset(TestUint64) << ",\n"
                << createCommandForDataset(TestInt32) << ",\n"
                << createCommandForDataset(TestInt64) << "]}";

  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandStream.str(), EmptyStreamHDFInfo);

  verifyWrittenDatatype(TestFile, TestUint32);
  verifyWrittenDatatype(TestFile, TestUint64);
  verifyWrittenDatatype(TestFile, TestInt32);
  verifyWrittenDatatype(TestFile, TestInt64);
}

TEST(HDFFile, createStaticDatasetsStrings) {
  std::string const hdf_output_filename =
      "Test.HDFFile.createStaticDatasetsStrings";
  unlink(hdf_output_filename.c_str());
  auto CommandJSON = json::parse(R""(
{
  "cmd": "FileWriter_new",
  "file_attributes": {
  },
  "nexus_structure": {
    "children": [
      {
        "type": "group",
        "name": "some_group",
        "attributes": {
          "NX_class": "NXinstrument"
        },
        "children": [
          {
            "type": "dataset",
            "name": "string_var_0d",
            "dataset": {
              "type": "string"
            },
            "values": "the-scalar-string"
          },
          {
            "type": "dataset",
            "name": "string_fix_0d",
            "dataset": {
              "type": "string",
              "string_size": 32
            },
            "values": "string_scalar"
          },
          {
            "type": "dataset",
            "name": "string_var_1d",
            "dataset": {
              "type": "string",
              "size": ["unlimited"]
            },
            "values": ["the-scalar-string", "another-one", "a-third"]
          },
          {
            "type": "dataset",
            "name": "string_fix_1d",
            "dataset": {
              "type": "string",
              "string_size": 32,
              "size": ["unlimited"]
            },
            "values": ["the-scalar-string", "another-one", "a-third"]
          },
          {
            "type": "dataset",
            "name": "string_var_2d",
            "dataset": {
              "type": "string",
              "size": ["unlimited", 4]
            },
            "values": [
              ["string_0_0", "string_0_1", "string_0_2", "string_0_3"],
              ["string_1_0", "string_1_1", "string_1_2", "string_1_3"],
              ["string_2_0", "string_2_1", "string_2_2", "string_2_3"],
              ["string_3_0", "string_3_1", "string_3_2", "string_3_3"]
            ]
          },
          {
            "type": "dataset",
            "name": "string_fix_2d",
            "dataset": {
              "type": "string",
              "string_size": 32,
              "size": ["unlimited", 4]
            },
            "values": [
              ["string_0_0", "string_0_1", "string_0_2", "string_0_3"],
              ["string_1_0", "string_1_1", "string_1_2", "string_1_3"],
              ["string_2_0", "string_2_1", "string_2_2", "string_2_3"],
              ["string_3_0", "string_3_1", "string_3_2", "string_3_3"]
            ]
          }
        ]
      }
    ]
  }
}
  )"");
  CommandJSON["file_attributes"]["file_name"] = hdf_output_filename;
  std::string Filename = CommandJSON["file_attributes"]["file_name"];
  ASSERT_GT(Filename.size(), 8u);
  std::vector<FileWriter::StreamHDFInfo> NoStreams;
  {
    FileWriter::HDFFile File;
    File.init(Filename, CommandJSON["nexus_structure"], json::object(),
              NoStreams, false);
  }
  {
    auto File = hdf5::file::open(Filename, hdf5::file::AccessFlags::READONLY);
    auto StringVar = hdf5::datatype::String::variable();
    StringVar.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    StringVar.padding(hdf5::datatype::StringPad::NULLTERM);
    auto StringFix = hdf5::datatype::String::fixed(32);
    StringFix.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    StringFix.padding(hdf5::datatype::StringPad::NULLTERM);
    hdf5::node::Dataset Dataset;
    Dataset = hdf5::node::get_dataset(File.root(), "/some_group/string_var_0d");
    ASSERT_EQ(Dataset.datatype(), StringVar);
    ASSERT_EQ(Dataset.dataspace().type(), hdf5::dataspace::Type::SCALAR);
    Dataset = hdf5::node::get_dataset(File.root(), "/some_group/string_fix_0d");
    ASSERT_EQ(Dataset.datatype(), StringFix);
    ASSERT_EQ(Dataset.dataspace().type(), hdf5::dataspace::Type::SCALAR);
    Dataset = hdf5::node::get_dataset(File.root(), "/some_group/string_var_1d");
    ASSERT_EQ(Dataset.datatype(), StringVar);
    ASSERT_EQ(Dataset.dataspace().type(), hdf5::dataspace::Type::SIMPLE);
    Dataset = hdf5::node::get_dataset(File.root(), "/some_group/string_fix_1d");
    ASSERT_EQ(Dataset.datatype(), StringFix);
    ASSERT_EQ(Dataset.dataspace().type(), hdf5::dataspace::Type::SIMPLE);
    {
      auto NewDataset =
          hdf5::node::get_dataset(File.root(), "/some_group/string_fix_2d");
      ASSERT_EQ(NewDataset.datatype(), StringFix);
      ASSERT_EQ(NewDataset.dataspace().type(), hdf5::dataspace::Type::SIMPLE);
      hdf5::dataspace::Simple NewSpaceFile = NewDataset.dataspace();
      NewSpaceFile.selection(hdf5::dataspace::SelectionOperation::SET,
                             hdf5::dataspace::Hyperslab({2, 1}, {1, 3}));
      std::vector<char> Buffer(3 * 32);

      hdf5::dataspace::Simple SpaceMem({3});
      if (0 > H5Dread(static_cast<hid_t>(NewDataset),
                      static_cast<hid_t>(StringFix),
                      static_cast<hid_t>(SpaceMem),
                      static_cast<hid_t>(NewSpaceFile), H5P_DEFAULT,
                      Buffer.data())) {
        ASSERT_TRUE(false);
      }
      ASSERT_EQ(std::string(Buffer.data() + 0 * 32), "string_2_1");
      ASSERT_EQ(std::string(Buffer.data() + 1 * 32), "string_2_2");
      ASSERT_EQ(std::string(Buffer.data() + 2 * 32), "string_2_3");
    }
    {
      auto OtherDataset =
          hdf5::node::get_dataset(File.root(), "/some_group/string_var_2d");
      ASSERT_EQ(OtherDataset.datatype(), StringVar);
      ASSERT_EQ(OtherDataset.dataspace().type(), hdf5::dataspace::Type::SIMPLE);
      hdf5::dataspace::Simple OtherSpaceFile = OtherDataset.dataspace();
      OtherSpaceFile.selection(hdf5::dataspace::SelectionOperation::SET,
                               hdf5::dataspace::Hyperslab({2, 1}, {1, 2}));
      std::vector<std::string> Buffer;
      Buffer.resize(2);
      OtherDataset.read(Buffer, StringVar, hdf5::dataspace::Simple({2}),
                        OtherSpaceFile);
      ASSERT_EQ(Buffer.at(0), "string_2_1");
      ASSERT_EQ(Buffer.at(1), "string_2_2");
    }
  }
}
