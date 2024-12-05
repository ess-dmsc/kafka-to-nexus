// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatBufferGenerators.h"
#include "Stream/SourceFilter.h"
#include "WriterModule/f144/f144_Writer.h"
#include <chrono>
#include <gtest/gtest.h>

class StubMessageWriter : public Stream::MessageWriter {
public:
  StubMessageWriter()
      : MessageWriter([]() {}, 1s, std::make_unique<Metrics::Registrar>("")) {}
  void addMessage(Stream::Message const &message,
                  [[maybe_unused]] bool is_buffered_message) override {
    messages_received.emplace_back(message);
  }
  std::vector<Stream::Message> messages_received;
};

FileWriter::FlatbufferMessage create_f144_message(std::string const &source,
                                                  double value,
                                                  int64_t timestamp_ms) {
  auto const [buffer, size] =
      FlatBuffers::create_f144_message_double(source, value, timestamp_ms);
  return {buffer.get(), size};
}

// Helper class to manage life times correctly
struct TestHarness {
  TestHarness(std::unique_ptr<StubMessageWriter> message_writer,
              std::unique_ptr<Metrics::Registrar> registrar,
              std::unique_ptr<WriterModule::f144::f144_Writer> writer_module,
              std::unique_ptr<Stream::SourceFilter> filter)
      : writer(std::move(message_writer)), registrar(std::move(registrar)),
        writer_module(std::move(writer_module)), filter(std::move(filter)) {}

  std::unique_ptr<StubMessageWriter> writer;
  std::unique_ptr<Metrics::Registrar> registrar;
  std::unique_ptr<WriterModule::f144::f144_Writer> writer_module;
  std::unique_ptr<Stream::SourceFilter> filter;
};

TestHarness create_filter_for_tests(time_point start_time = time_point::min(),
                                    time_point stop_time = time_point::max(),
                                    bool allow_repeated = false) {
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  auto filter = std::make_unique<Stream::SourceFilter>(
      start_time, stop_time, allow_repeated, writer.get(),
      std::move(registrar));
  filter->set_source_hash(FileWriter::calcSourceHash("f144", "::source::"));
  filter->add_writer_module_for_message(f144_writer.get());
  return {std::move(writer), std::move(registrar), std::move(f144_writer),
          std::move(filter)};
}

TEST(SourceFilter, messages_within_start_and_stop_are_allowed_through) {
  auto harness = create_filter_for_tests();

  harness.filter->filter_message(create_f144_message("::source::", 1, 100));
  harness.filter->filter_message(create_f144_message("::source::", 2, 200));

  EXPECT_EQ(2u, harness.writer->messages_received.size());
}

TEST(SourceFilter, messages_with_wrong_hash_are_ignored) {
  auto harness = create_filter_for_tests();

  harness.filter->filter_message(
      create_f144_message("wrong_source_give_wrong_hash", 1, 100));
  harness.filter->filter_message(
      create_f144_message("wrong_source_give_wrong_hash", 2, 200));

  EXPECT_EQ(0u, harness.writer->messages_received.size());
}

TEST(SourceFilter, out_of_order_messages_are_allowed_through) {
  auto harness = create_filter_for_tests();

  harness.filter->filter_message(create_f144_message("::source::", 2, 200));
  harness.filter->filter_message(create_f144_message("::source::", 1, 100));

  EXPECT_EQ(2u, harness.writer->messages_received.size());
}

TEST(SourceFilter, invalid_message_is_filtered_out) {
  auto harness = create_filter_for_tests();

  FileWriter::FlatbufferMessage invalid;
  harness.filter->filter_message(invalid);

  EXPECT_EQ(0u, harness.writer->messages_received.size());
}

TEST(SourceFilter, message_before_start_is_not_allowed_through) {
  auto harness = create_filter_for_tests(time_point{1000ms});

  harness.filter->filter_message(create_f144_message("::source::", 1, 100));

  EXPECT_EQ(0u, harness.writer->messages_received.size());
}

TEST(SourceFilter, message_on_start_is_allowed_through) {
  auto harness = create_filter_for_tests(time_point{1000ms});

  harness.filter->filter_message(create_f144_message("::source::", 1, 1000));

  EXPECT_EQ(1u, harness.writer->messages_received.size());
}

TEST(SourceFilter, messages_after_stop_are_not_allowed_through) {
  auto harness = create_filter_for_tests(time_point{0ms}, time_point{1000ms});

  harness.filter->filter_message(create_f144_message("::source::", 1, 1001));
  harness.filter->filter_message(create_f144_message("::source::", 2, 1002));

  EXPECT_EQ(0u, harness.writer->messages_received.size());
}

TEST(SourceFilter, message_after_stop_sets_filter_to_finished) {
  auto harness = create_filter_for_tests(time_point{0ms}, time_point{1000ms});

  harness.filter->filter_message(create_f144_message("::source::", 1, 1001));

  EXPECT_EQ(true, harness.filter->has_finished());
}

TEST(SourceFilter,
     messages_with_same_timestamp_ignored_when_allowed_repeated_is_false) {
  auto harness =
      create_filter_for_tests(time_point{0ms}, time_point{1000ms}, false);

  harness.filter->filter_message(create_f144_message("::source::", 1, 1000));
  harness.filter->filter_message(create_f144_message("::source::", 2, 1000));
  harness.filter->filter_message(create_f144_message("::source::", 3, 1000));

  EXPECT_EQ(1u, harness.writer->messages_received.size());
}

TEST(SourceFilter,
     messages_with_same_timestamp_allowed_when_allowed_repeated_is_true) {
  auto harness =
      create_filter_for_tests(time_point{0ms}, time_point{1000ms}, true);

  harness.filter->filter_message(create_f144_message("::source::", 1, 1000));
  harness.filter->filter_message(create_f144_message("::source::", 2, 1000));
  harness.filter->filter_message(create_f144_message("::source::", 3, 1000));

  EXPECT_EQ(3u, harness.writer->messages_received.size());
}

TEST(SourceFilter, can_change_stop_time_after_construction) {
  auto harness = create_filter_for_tests();

  harness.filter->set_stop_time(time_point{1000ms});

  // Messages after stop are not allowed
  harness.filter->filter_message(create_f144_message("::source::", 1, 999));
  harness.filter->filter_message(create_f144_message("::source::", 1, 1000));
  harness.filter->filter_message(create_f144_message("::source::", 1, 1001));
  harness.filter->filter_message(create_f144_message("::source::", 2, 1002));

  EXPECT_EQ(2u, harness.writer->messages_received.size());
}

TEST(SourceFilter,
     last_message_before_start_time_is_allowed_through_after_valid_message) {
  // For values that don't update very often, the forwarder periodically sends
  // the current value to Kafka. This is to ensure that the data file contains
  // the value despite it not changing.
  auto harness = create_filter_for_tests(time_point{1000ms});

  harness.filter->filter_message(create_f144_message("::source::", 1, 100));
  harness.filter->filter_message(create_f144_message("::source::", 2, 200));
  harness.filter->filter_message(create_f144_message("::source::", 3, 1002));

  EXPECT_EQ(2u, harness.writer->messages_received.size());
  auto first = harness.writer->messages_received.at(0).FbMsg;
  EXPECT_EQ(200000000, first.getTimestamp()); // timestamp is in ns
}

TEST(SourceFilter,
     last_message_before_start_time_handles_out_of_order_messages) {
  // For values that don't update very often, the forwarder periodically sends
  // the current value to Kafka. This is to ensure that the data file contains
  // the value despite it not changing.
  auto harness = create_filter_for_tests(time_point{1000ms});

  harness.filter->filter_message(create_f144_message("::source::", 2, 200));
  harness.filter->filter_message(create_f144_message("::source::", 1, 100));
  harness.filter->filter_message(create_f144_message("::source::", 3, 1002));

  EXPECT_EQ(2u, harness.writer->messages_received.size());
  auto first = harness.writer->messages_received.at(0).FbMsg;
  EXPECT_EQ(200000000, first.getTimestamp()); // timestamp is in ns
}

TEST(
    SourceFilter,
    last_message_before_start_time_is_allowed_through_on_destruction_if_no_updates) {
  // For values that don't update very often, the forwarder periodically sends
  // the current value to Kafka. This is to ensure that the data file contains
  // the value despite it not changing.
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  {
    Stream::SourceFilter filter{time_point{1000ms}, time_point::max(), false,
                                writer.get(), std::move(registrar)};
    filter.set_source_hash(FileWriter::calcSourceHash("f144", "::source::"));
    filter.add_writer_module_for_message(f144_writer.get());

    filter.filter_message(create_f144_message("::source::", 2, 200));
  }

  EXPECT_EQ(1u, writer->messages_received.size());
  auto first = writer->messages_received.at(0).FbMsg;
  EXPECT_EQ(200000000, first.getTimestamp()); // timestamp is in ns
}

TEST(
    SourceFilter,
    last_message_before_start_time_is_not_allowed_through_on_destruction_if_already_written) {
  // For values that don't update very often, the forwarder periodically sends
  // the current value to Kafka. This is to ensure that the data file contains
  // the value despite it not changing.
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  {
    Stream::SourceFilter filter{time_point{1000ms}, time_point::max(), false,
                                writer.get(), std::move(registrar)};
    filter.set_source_hash(FileWriter::calcSourceHash("f144", "::source::"));
    filter.add_writer_module_for_message(f144_writer.get());

    filter.filter_message(create_f144_message("::source::", 2, 200));
    filter.filter_message(create_f144_message("::source::", 3, 1002));
  }

  EXPECT_EQ(2u, writer->messages_received.size());
  auto first = writer->messages_received.at(0).FbMsg;
  EXPECT_EQ(200000000, first.getTimestamp()); // timestamp is in ns
}

TEST(SourceFilter, messages_written_for_each_module_when_more_than_one_module) {
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer_1 = std::make_unique<WriterModule::f144::f144_Writer>();
  auto f144_writer_2 = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{0ms}, time_point::max(), false,
                              writer.get(), std::move(registrar)};
  filter.set_source_hash(FileWriter::calcSourceHash("f144", "::source::"));
  filter.add_writer_module_for_message(f144_writer_1.get());
  filter.add_writer_module_for_message(f144_writer_2.get());

  filter.filter_message(create_f144_message("::source::", 1, 100));
  filter.filter_message(create_f144_message("::source::", 2, 200));

  EXPECT_EQ(4u, writer->messages_received.size());
}
