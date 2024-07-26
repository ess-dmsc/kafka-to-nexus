// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Stream/SourceFilter.h"
#include "WriterModule/f144/f144_Writer.h"
#include <chrono>
#include <f144_logdata_generated.h>
#include <gtest/gtest.h>

class StubMessageWriter : public Stream::MessageWriter {
public:
  StubMessageWriter()
      : MessageWriter([]() {}, 1s, std::make_unique<Metrics::Registrar>("")) {}
  void addMessage(Stream::Message const &Msg) override {
    messages_received.emplace_back(Msg);
  }
  std::vector<Stream::Message> messages_received;
};

std::pair<std::unique_ptr<uint8_t[]>, size_t>
create_f144_message_double(std::string const &source, double value,
                           int64_t timestamp_ms) {
  auto builder = flatbuffers::FlatBufferBuilder();
  auto source_name_offset = builder.CreateString(source);
  auto value_offset = CreateDouble(builder, value).Union();

  f144_LogDataBuilder f144_builder(builder);
  f144_builder.add_value(value_offset);
  f144_builder.add_source_name(source_name_offset);
  f144_builder.add_timestamp(timestamp_ms * 1000000);
  f144_builder.add_value_type(Value::Double);
  Finishf144_LogDataBuffer(builder, f144_builder.Finish());

  size_t buffer_size = builder.GetSize();
  auto buffer = std::make_unique<uint8_t[]>(buffer_size);
  std::memcpy(buffer.get(), builder.GetBufferPointer(), buffer_size);
  return {std::move(buffer), buffer_size};
}

FileWriter::FlatbufferMessage create_f144_message(std::string const &source,
                                                  double value,
                                                  int64_t timestamp_ms) {
  auto const [buffer, size] =
      create_f144_message_double(source, value, timestamp_ms);
  return {buffer.get(), size};
}

TEST(SourceFilter, messages_within_start_and_stop_are_allowed_through) {
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{0ms}, time_point::max(), false,
                              writer.get(), std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  filter.filter_message(create_f144_message("::source::", 1, 100));
  filter.filter_message(create_f144_message("::source::", 2, 200));

  EXPECT_EQ(2u, writer->messages_received.size());
}

TEST(SourceFilter, out_of_order_messages_are_allowed_through) {
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{0ms}, time_point::max(), false,
                              writer.get(), std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  filter.filter_message(create_f144_message("::source::", 2, 200));
  filter.filter_message(create_f144_message("::source::", 1, 100));

  EXPECT_EQ(2u, writer->messages_received.size());
}

TEST(SourceFilter, invalid_message_is_filtered_out) {
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{0ms}, time_point::max(), false,
                              writer.get(), std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  FileWriter::FlatbufferMessage invalid;
  filter.filter_message(invalid);

  EXPECT_EQ(0u, writer->messages_received.size());
}

TEST(SourceFilter, message_before_start_is_not_allowed_through) {
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{1000ms}, time_point::max(), false,
                              writer.get(), std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  filter.filter_message(create_f144_message("::source::", 1, 100));

  EXPECT_EQ(0u, writer->messages_received.size());
}

TEST(SourceFilter, message_on_start_is_allowed_through) {
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{1000ms}, time_point::max(), false,
                              writer.get(), std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  filter.filter_message(create_f144_message("::source::", 1, 1000));

  EXPECT_EQ(1u, writer->messages_received.size());
}

TEST(SourceFilter, first_message_after_stop_is_allowed_through) {
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{0ms}, time_point{1000ms}, false,
                              writer.get(), std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  filter.filter_message(create_f144_message("::source::", 1, 1001));
  filter.filter_message(create_f144_message("::source::", 1, 1002));

  EXPECT_EQ(1u, writer->messages_received.size());
}

TEST(SourceFilter, message_after_stop_sets_filter_to_finished) {
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{0ms}, time_point{1000ms}, false,
                              writer.get(), std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  filter.filter_message(create_f144_message("::source::", 1, 1001));

  EXPECT_EQ(true, filter.has_finished());
}

TEST(SourceFilter,
     messages_with_same_timestamp_ignored_when_allowed_repeated_is_false) {
  auto allow_repeated_timestamps = false;
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{0ms}, time_point::max(),
                              allow_repeated_timestamps, writer.get(),
                              std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  filter.filter_message(create_f144_message("::source::", 1, 1000));
  filter.filter_message(create_f144_message("::source::", 2, 1000));
  filter.filter_message(create_f144_message("::source::", 3, 1000));

  EXPECT_EQ(1u, writer->messages_received.size());
}

TEST(SourceFilter,
     messages_with_same_timestamp_allowed_when_allowed_repeated_is_true) {
  auto allow_repeated_timestamps = true;
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{0ms}, time_point::max(),
                              allow_repeated_timestamps, writer.get(),
                              std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  filter.filter_message(create_f144_message("::source::", 1, 1000));
  filter.filter_message(create_f144_message("::source::", 2, 1000));
  filter.filter_message(create_f144_message("::source::", 3, 1000));

  EXPECT_EQ(3u, writer->messages_received.size());
}

TEST(SourceFilter, can_change_stop_time_after_construction) {
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{0ms}, time_point::max(), false,
                              writer.get(), std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  filter.set_stop_time(time_point{1000ms});

  // First message after stop is allowed
  filter.filter_message(create_f144_message("::source::", 1, 1001));
  filter.filter_message(create_f144_message("::source::", 2, 1002));

  EXPECT_EQ(1u, writer->messages_received.size());
}

TEST(SourceFilter,
     last_message_before_start_time_is_allowed_through_after_valid_message) {
  // For values that don't update very often, the forwarder periodically sends
  // the current value to Kafka. This is to ensure that the data file contains
  // the value despite it not changing.
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{1000ms}, time_point::max(), false,
                              writer.get(), std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  filter.filter_message(create_f144_message("::source::", 1, 100));
  filter.filter_message(create_f144_message("some_source", 2, 200));
  filter.filter_message(create_f144_message("::source::", 3, 1002));

  EXPECT_EQ(2u, writer->messages_received.size());
  auto first = writer->messages_received.at(0).FbMsg;
  EXPECT_EQ(200000000, first.getTimestamp()); // timestamp is in ns
  EXPECT_EQ("some_source", first.getSourceName());
}

TEST(SourceFilter,
     last_message_before_start_time_handles_out_of_order_messages) {
  // For values that don't update very often, the forwarder periodically sends
  // the current value to Kafka. This is to ensure that the data file contains
  // the value despite it not changing.
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{1000ms}, time_point::max(), false,
                              writer.get(), std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer.get());

  filter.filter_message(create_f144_message("some_source", 2, 200));
  filter.filter_message(create_f144_message("::source::", 1, 100));
  filter.filter_message(create_f144_message("::source::", 3, 1002));

  EXPECT_EQ(2u, writer->messages_received.size());
  auto first = writer->messages_received.at(0).FbMsg;
  EXPECT_EQ(200000000, first.getTimestamp()); // timestamp is in ns
  EXPECT_EQ("some_source", first.getSourceName());
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
    filter.add_writer_module_for_message(f144_writer.get());

    filter.filter_message(create_f144_message("some_source", 2, 200));
  }

  EXPECT_EQ(1u, writer->messages_received.size());
  auto first = writer->messages_received.at(0).FbMsg;
  EXPECT_EQ(200000000, first.getTimestamp()); // timestamp is in ns
  EXPECT_EQ("some_source", first.getSourceName());
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
    filter.add_writer_module_for_message(f144_writer.get());

    filter.filter_message(create_f144_message("some_source", 2, 200));
    filter.filter_message(create_f144_message("::source::", 3, 1002));
  }

  EXPECT_EQ(2u, writer->messages_received.size());
  auto first = writer->messages_received.at(0).FbMsg;
  EXPECT_EQ(200000000, first.getTimestamp()); // timestamp is in ns
  EXPECT_EQ("some_source", first.getSourceName());
}

TEST(SourceFilter, messages_written_for_each_module_when_more_than_one_module) {
  auto writer = std::make_unique<StubMessageWriter>();
  auto registrar = std::make_unique<Metrics::Registrar>("");
  auto f144_writer_1 = std::make_unique<WriterModule::f144::f144_Writer>();
  auto f144_writer_2 = std::make_unique<WriterModule::f144::f144_Writer>();
  Stream::SourceFilter filter{time_point{0ms}, time_point::max(), false,
                              writer.get(), std::move(registrar)};
  filter.add_writer_module_for_message(f144_writer_1.get());
  filter.add_writer_module_for_message(f144_writer_2.get());

  filter.filter_message(create_f144_message("::source::", 1, 100));
  filter.filter_message(create_f144_message("::source::", 2, 200));

  EXPECT_EQ(4u, writer->messages_received.size());
}
