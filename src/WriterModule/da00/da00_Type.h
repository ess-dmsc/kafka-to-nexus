
#pragma once
#include <cstdint>
#include <string>

#include <da00_dataarray_generated.h>
#include <fmt/core.h>
#include <h5cpp/hdf5.hpp>
#include <nlohmann/json.hpp>

namespace WriterModule::da00 {

da00_dtype string_to_da00_dtype(const std::string &name);
const char *da00_dtype_to_string(const da00_dtype &type);

da00_dtype guess_dtype(const nlohmann::json &data);
std::vector<hsize_t> get_shape(const nlohmann::json &data);
//
template <class T> da00_dtype get_dtype(T) {
  throw std::runtime_error("Unknown type");
}
} // namespace WriterModule::da00

template <> struct fmt::formatter<da00_dtype> {
  template <typename ParseContext> constexpr auto parse(ParseContext &ctx) {
    return ctx.begin();
  }
  template <typename FormatContext>
  auto format(const da00_dtype &type, FormatContext &ctx) {
    return format_to(ctx.out(), "da00_dtype({})",
                     WriterModule::da00::da00_dtype_to_string(type));
  }
};