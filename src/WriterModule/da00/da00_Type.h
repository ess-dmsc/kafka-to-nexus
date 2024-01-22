#pragma once
#include <cstdint>
#include <string>
#include "json.h"

#include <da00_dataarray_generated.h>
#include <fmt/core.h>

namespace WriterModule::da00 {

enum class da00_type {
  none, int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64, c_string,
};

da00_type string_to_da00_type(const std::string & name);
const char *da00_type_to_string(const da00_type & type);

DType da00_type_to_dtype(const da00_type & type);
da00_type dtype_to_da00_type(const DType & type);
//
template<class T> DType get_dtype(T){throw std::runtime_error("Unknown type");}
} // namespace WriterModule::da00

template<> struct fmt::formatter<WriterModule::da00::da00_type> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }
    template<typename FormatContext>
    auto format(const WriterModule::da00::da00_type &type, FormatContext &ctx) {
      return format_to(ctx.out(), "da00_type({})",
                       WriterModule::da00::da00_type_to_string(type));
    }
};

template<> struct fmt::formatter<DType> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }
    template<typename FormatContext>
    auto format(const DType &type, FormatContext &ctx) {
      auto a_type = WriterModule::da00::dtype_to_da00_type(type);
      return format_to(ctx.out(), "Dtype({})",
                       WriterModule::da00::da00_type_to_string(a_type));
    }
};