// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <FlatBufferGenerators.h>
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
  }
]
)";

std::unique_ptr<uint8_t[]>
convert_to_raw_flatbuffer(nlohmann::json const &item) {
  std::string const schema = item["schema"];
  if (schema == "f144") {
    auto [buffer, size] = FlatBuffers::create_f144_message_double(
        item["source_name"], item["value"], item["timestamp"]);
    return std::move(buffer);
  }
  throw std::runtime_error("Unknown schema");
}

TEST(json_to_fb, can_create_f144_buffer) {
  auto data = json::parse(example_json);
  auto item = data[0];
  auto buffer = convert_to_raw_flatbuffer(item);
  auto fb = Getf144_LogData(buffer.get());
  ASSERT_EQ("test_source", fb->source_name()->str());
}
