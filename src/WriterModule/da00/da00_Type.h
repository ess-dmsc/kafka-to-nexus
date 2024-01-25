#pragma once
#include <cstdint>
#include <string>

#include <da00_dataarray_generated.h>
#include <fmt/core.h>

namespace WriterModule::da00 {

da00_dtype string_to_da00_dtype(const std::string &name);
const char *da00_dtype_to_string(const da00_dtype &type);

//
template<class T> da00_dtype get_dtype(T){throw std::runtime_error("Unknown type");}
} // namespace WriterModule::da00

template<> struct fmt::formatter<da00_dtype> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }
    template<typename FormatContext>
    auto format(const da00_dtype &type, FormatContext &ctx) {
      return format_to(ctx.out(), "da00_dtype({})",
                       WriterModule::da00::da00_dtype_to_string(type));
    }
};
