
#pragma once
#include <cstdint>
#include <string>

#include <da00_dataarray_generated.h>
#include <fmt/core.h>
#include <h5cpp/hdf5.hpp>
#include <nlohmann/json.hpp>

namespace WriterModule::da00 {

da00_dtype string_to_da00_dtype(std::string const &name);
const char *da00_dtype_to_string(da00_dtype const &type);

da00_dtype guess_dtype(nlohmann::json const &data);
std::vector<hsize_t> get_shape(nlohmann::json const &data);
//
template <class T> da00_dtype get_dtype(T) {
  if constexpr (std::is_same_v<T, int8_t>) {
    return da00_dtype::int8;
  }
  if constexpr (std::is_same_v<T, uint8_t>) {
    return da00_dtype::uint8;
  }
  if constexpr (std::is_same_v<T, int16_t>) {
    return da00_dtype::int16;
  }
  if constexpr (std::is_same_v<T, uint16_t>) {
    return da00_dtype::uint16;
  }
  if constexpr (std::is_same_v<T, int32_t>) {
    return da00_dtype::int32;
  }
  if constexpr (std::is_same_v<T, uint32_t>) {
    return da00_dtype::uint32;
  }
  if constexpr (std::is_same_v<T, int64_t>) {
    return da00_dtype::int64;
  }
  if constexpr (std::is_same_v<T, uint64_t>) {
    return da00_dtype::uint64;
  }
  if constexpr (std::is_same_v<T, float>) {
    return da00_dtype::float32;
  }
  if constexpr (std::is_same_v<T, double>) {
    return da00_dtype::float64;
  }
  if constexpr (std::is_same_v<T, std::string>) {
    return da00_dtype::c_string;
  }
  throw std::runtime_error("Unknown type provided to WriterModule::da00::get_dtype");
}
} // namespace WriterModule::da00

template <> struct fmt::formatter<da00_dtype> {
  template <typename ParseContext> constexpr auto parse(ParseContext &ctx) {
    return ctx.begin();
  }
  template <typename FormatContext>
  auto format(da00_dtype const &type, FormatContext &ctx) {
    return format_to(ctx.out(), "da00_dtype({})",
                     WriterModule::da00::da00_dtype_to_string(type));
  }
};
