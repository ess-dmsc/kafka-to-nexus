// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <FlatBufferGenerators.h>
#include <da00_dataarray_generated.h>
#include <ev44_events_generated.h>
#include <f144_logdata_generated.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

std::string const example_json = R"(
[
  {
     "schema": "f144",
     "source_name": "test_source",
     "timestamp": 123456,
     "value": 3.14
  },
  {
     "schema": "ev44",
     "source_name": "test_source",
     "message_id": 666,
     "reference_time": 123456,
     "time_of_flight": [10, 20, 30],
     "pixel_ids": [1, 2, 3]
  },
  {
    "schema": "da00",
    "topic": "local_detector",
    "kafka_timestamp": 10600,
    "source_name": "monitor_data",
    "timestamp": 10600,
    "name": "signal",
    "axis_name": "x",
    "data": [4, 3, 2, 1, 2, 3, 4]
  }
]
)";

TEST(json_to_fb, can_create_f144_buffer) {
  json data = json::parse(example_json);
  json item = data[0];
  std::pair<std::unique_ptr<uint8_t[]>, size_t> raw_flatbuffer =
      FlatBuffers::convert_to_raw_flatbuffer(item);
  uint8_t *buffer = raw_flatbuffer.first.get();
  auto fb = Getf144_LogData(buffer);
  ASSERT_EQ("test_source", fb->source_name()->str());
  ASSERT_EQ(123456000000, fb->timestamp());
  ASSERT_EQ(3.14, fb->value_as_Double()->value());
}

TEST(json_to_fb, can_create_ev44_buffer) {
  json data = json::parse(example_json);
  json item = data[1];
  std::pair<std::unique_ptr<uint8_t[]>, size_t> raw_flatbuffer =
      FlatBuffers::convert_to_raw_flatbuffer(item);
  uint8_t *buffer = raw_flatbuffer.first.get();
  auto fb = GetEvent44Message(buffer);
  ASSERT_EQ("test_source", fb->source_name()->str());
  ASSERT_EQ(666, fb->message_id());
  ASSERT_EQ(123456000000, fb->reference_time()->Get(0));
  ASSERT_EQ(10, fb->time_of_flight()->Get(0));
  ASSERT_EQ(20, fb->time_of_flight()->Get(1));
  ASSERT_EQ(30, fb->time_of_flight()->Get(2));
  ASSERT_EQ(1, fb->pixel_id()->Get(0));
  ASSERT_EQ(2, fb->pixel_id()->Get(1));
  ASSERT_EQ(3, fb->pixel_id()->Get(2));
}

TEST(json_to_fb, can_create_da00_buffer) {
  json data = json::parse(example_json);
  json item = data[2];
  std::pair<std::unique_ptr<uint8_t[]>, size_t> raw_flatbuffer =
      FlatBuffers::convert_to_raw_flatbuffer(item);
  uint8_t *buffer = raw_flatbuffer.first.get();
  auto fb = Getda00_DataArray(buffer);
  auto variable = fb->data()->cbegin();
  auto data_array = reinterpret_cast<const int32_t *>(variable->data()->data());
  ASSERT_EQ("monitor_data", fb->source_name()->str());
  ASSERT_EQ(10600000000, fb->timestamp());
  ASSERT_EQ("signal", variable->name()->str());
  ASSERT_EQ("x", variable->axes()->cbegin()->str());
  ASSERT_EQ(4, variable->data()->Get(0));
  ASSERT_EQ(7, variable->shape()->Get(0));
  ASSERT_EQ(da00_dtype::int32, variable->data_type());
  ASSERT_EQ(4, data_array[0]);
  ASSERT_EQ(3, data_array[1]);
  ASSERT_EQ(4, data_array[6]);
}

// TODO: ep00, al00 and ad00 tests needed
