// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <da00_dataarray_generated.h>
#include <ep01_epics_connection_generated.h>
#include <ev44_events_generated.h>
#include <f144_logdata_generated.h>

namespace FlatBuffers {

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

std::pair<std::unique_ptr<uint8_t[]>, size_t>
create_f144_message_array_double(std::string const &source,
                                 const std::vector<double> &values,
                                 int64_t timestamp_ms) {
  auto builder = flatbuffers::FlatBufferBuilder();
  auto source_name_offset = builder.CreateString(source);
  auto values_offset = CreateArrayDoubleDirect(builder, &values).Union();

  f144_LogDataBuilder f144_builder(builder);
  f144_builder.add_value(values_offset);
  f144_builder.add_source_name(source_name_offset);
  f144_builder.add_timestamp(timestamp_ms * 1000000);
  f144_builder.add_value_type(Value::ArrayDouble);
  Finishf144_LogDataBuffer(builder, f144_builder.Finish());

  size_t buffer_size = builder.GetSize();
  auto buffer = std::make_unique<uint8_t[]>(buffer_size);
  std::memcpy(buffer.get(), builder.GetBufferPointer(), buffer_size);
  return {std::move(buffer), buffer_size};
}

std::pair<std::unique_ptr<uint8_t[]>, size_t>
create_ep01_message_double(std::string const &source, ConnectionInfo status,
                           int64_t timestamp_ms) {
  auto builder = flatbuffers::FlatBufferBuilder();
  auto source_name_offset = builder.CreateString(source);

  EpicsPVConnectionInfoBuilder ep01_builder(builder);
  ep01_builder.add_source_name(source_name_offset);
  ep01_builder.add_timestamp(timestamp_ms);
  ep01_builder.add_status(status);
  FinishEpicsPVConnectionInfoBuffer(builder, ep01_builder.Finish());

  size_t buffer_size = builder.GetSize();
  auto buffer = std::make_unique<uint8_t[]>(buffer_size);
  std::memcpy(buffer.get(), builder.GetBufferPointer(), buffer_size);
  return {std::move(buffer), buffer_size};
}

std::pair<std::unique_ptr<uint8_t[]>, size_t>
create_ev44_message(std::string const &source, int64_t message_id,
                    int64_t timestamp_ns,
                    std::vector<int32_t> const &time_of_flight,
                    std::vector<int32_t> const &pixel_ids) {
  auto builder = flatbuffers::FlatBufferBuilder();
  auto source_name_offset = builder.CreateString(source);

  std::vector<int64_t> reference_times = {timestamp_ns};
  auto reference_time_offset = builder.CreateVector(reference_times);
  std::vector<int32_t> reference_index = {0};
  auto reference_index_offset = builder.CreateVector(reference_index);

  auto time_of_flight_offset = builder.CreateVector(time_of_flight);
  auto pixel_ids_offset = builder.CreateVector(pixel_ids);

  Event44MessageBuilder ev44_builder(builder);
  ev44_builder.add_source_name(source_name_offset);
  ev44_builder.add_message_id(message_id);
  ev44_builder.add_reference_time(reference_time_offset);
  ev44_builder.add_reference_time_index(reference_index_offset);
  ev44_builder.add_time_of_flight(time_of_flight_offset);
  ev44_builder.add_pixel_id(pixel_ids_offset);
  FinishEvent44MessageBuffer(builder, ev44_builder.Finish());

  size_t buffer_size = builder.GetSize();
  auto buffer = std::make_unique<uint8_t[]>(buffer_size);
  std::memcpy(buffer.get(), builder.GetBufferPointer(), buffer_size);
  return {std::move(buffer), buffer_size};
}

std::pair<std::unique_ptr<uint8_t[]>, size_t>
create_da00_message_int32s(std::string const &source, int64_t timestamp_ms,
                           const std::vector<int32_t> &data) {
  auto builder = flatbuffers::FlatBufferBuilder();
  builder.ForceDefaults(true);

  auto source_name_offset = builder.CreateString(source);
  auto var_name_offset = builder.CreateString("value");

  auto var_axis = builder.CreateString("x");
  std::vector<flatbuffers::Offset<flatbuffers::String>> var_axes_offset = {
      var_axis};
  auto var_axes = builder.CreateVector(var_axes_offset);

  std::vector<int64_t> var_shape = {static_cast<int64_t>(data.size())};
  auto var_shape_offset = builder.CreateVector(var_shape);

  std::uint8_t *p_data;
  auto payload =
      builder.CreateUninitializedVector(data.size(), sizeof(data[0]), &p_data);
  std::memcpy(p_data, data.data(), sizeof(data[0]) * data.size());

  auto variable_offset =
      Createda00_Variable(builder, var_name_offset, 0, 0, 0, da00_dtype::int32,
                          var_axes, var_shape_offset, payload);
  std::vector<flatbuffers::Offset<da00_Variable>> variable_offsets = {
      variable_offset};
  auto variables = builder.CreateVector(variable_offsets);

  auto da00 = Createda00_DataArray(builder, source_name_offset, timestamp_ms,
                                   variables);
  builder.Finish(da00, "da00");

  auto verifier =
      flatbuffers::Verifier(builder.GetBufferPointer(), builder.GetSize());
  if (!Verifyda00_DataArrayBuffer(verifier)) {
    throw std::runtime_error("could not verify da00");
  }

  size_t buffer_size = builder.GetSize();
  auto buffer = std::make_unique<uint8_t[]>(buffer_size);
  std::memcpy(buffer.get(), builder.GetBufferPointer(), buffer_size);
  return {std::move(buffer), buffer_size};
}

} // namespace FlatBuffers