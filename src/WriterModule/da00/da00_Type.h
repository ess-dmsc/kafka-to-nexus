
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
  throw std::runtime_error("Unknown type");
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
