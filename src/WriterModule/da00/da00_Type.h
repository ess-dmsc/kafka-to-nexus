#pragma once
#include <cstdint>
#include <string>
#include "json.h"

#include <da00_dataarray_generated.h>
#include <fmt/core.h>

namespace WriterModule::da00 {

using AllowedTypes = std::tuple<
    int8_t, int16_t, int32_t, uint8_t, uint16_t, uint32_t, float, double, char
>;
enum class da00_type {
  none, int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64, c_string,
};

template <class T> struct ValueType {
    static constexpr const char *value = "unknown";
    static constexpr const da00_type buffered = da00_type::none;
    using type = void;
};
template<> struct ValueType<int8_t> {
    static constexpr const char *value = "int8";
    static constexpr const da00_type buffered = da00_type::int8;
    using type = int8_t;
};
template<> struct ValueType<uint8_t> {
    static constexpr const char *value = "uint8";
    static constexpr const da00_type buffered = da00_type::uint8;
    using type = uint8_t;
};
template<> struct ValueType<int16_t> {
    static constexpr const char *value = "int16";
    static constexpr const da00_type buffered = da00_type::int16;
    using type = int16_t;
};
template<> struct ValueType<uint16_t> {
    static constexpr const char *value = "uint16";
    static constexpr const da00_type buffered = da00_type::uint16;
    using type = uint16_t;
};
template<> struct ValueType<int32_t> {
    static constexpr const char *value = "int32";
    static constexpr const da00_type buffered = da00_type::int32;
    using type = int32_t;
};
template<> struct ValueType<uint32_t> {
    static constexpr const char *value = "uint32";
    static constexpr const da00_type buffered = da00_type::uint32;
    using type = uint32_t;
};
template<> struct ValueType<int64_t> {
    static constexpr const char *value = "int64";
    static constexpr const da00_type buffered = da00_type::int64;
    using type = int64_t;
};
template<> struct ValueType<uint64_t> {
    static constexpr const char *value = "uint64";
    static constexpr const da00_type buffered = da00_type::uint64;
    using type = uint64_t;
};
template<> struct ValueType<float> {
    static constexpr const char *value = "float32";
    static constexpr const da00_type buffered = da00_type::float32;
    using type = float;
};
template<> struct ValueType<double> {
    static constexpr const char *value = "float64";
    static constexpr const da00_type buffered = da00_type::float64;
    using type = double;
};
template<> struct ValueType<char> {
    static constexpr const char *value = "c_string";
    static constexpr const da00_type buffered = da00_type::c_string;
    using type = char;
};

template<class T> using ValueType_t = typename ValueType<T>::type;
template<class T> using ValueType_s = typename ValueType<T>::value;
template<class T> using ValueType_b = typename ValueType<T>::buffered;

template<size_t N> struct DTypeToValueType {using type = void;};
template<> struct DTypeToValueType<1u> {using type = int8_t;};
template<> struct DTypeToValueType<2u> {using type = uint8_t;};
template<> struct DTypeToValueType<3u> {using type = int16_t;};
template<> struct DTypeToValueType<4u> {using type = uint16_t;};
template<> struct DTypeToValueType<5u> {using type = int32_t;};
template<> struct DTypeToValueType<6u> {using type = uint32_t;};
template<> struct DTypeToValueType<7u> {using type = int64_t;};
template<> struct DTypeToValueType<8u> {using type = uint64_t;};
template<> struct DTypeToValueType<9u> {using type = float;};
template<> struct DTypeToValueType<10u> {using type = double;};
template<> struct DTypeToValueType<11u> {using type = char;};
template<> struct DTypeToValueType<12u> {using type = void;};
template<size_t N> using DTypeToValueType_t = typename DTypeToValueType<N>::type;


template<class, class> struct is_one_of;
template<class T> struct is_one_of<T, std::tuple<>> : std::false_type{};
template<class T, class Head, class... Tail>
struct is_one_of<T, std::tuple<Head, Tail...>>
  : std::integral_constant<bool, std::is_same_v<T, Head> || is_one_of<T,Tail...>::value> {};

template<class T, class S> using enable_if_allowed_t = std::enable_if_t<is_one_of<T, AllowedTypes>::value, S>;

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