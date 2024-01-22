#include "da00_Type.h"

#include <sys/stat.h>

using namespace WriterModule::da00;

da00_type WriterModule::da00::string_to_da00_type(const std::string & name) {
  if (name == "none") return da00_type::none;
  if (name == "int8") return da00_type::int8;
  if (name == "uint8") return da00_type::uint8;
  if (name == "int16") return da00_type::int16;
  if (name == "uint16") return da00_type::uint16;
  if (name == "int32") return da00_type::int32;
  if (name == "uint32") return da00_type::uint32;
  if (name == "int64") return da00_type::int64;
  if (name == "uint64") return da00_type::uint64;
  if (name == "float32") return da00_type::float32;
  if (name == "float64") return da00_type::float64;
  if (name == "c_string") return da00_type::c_string;
  return da00_type::none;
}

const char * WriterModule::da00::da00_type_to_string(const da00_type &type) {
  switch (type) {
    case da00_type::int8: return "int8";
    case da00_type::uint8: return "uint8";
    case da00_type::int16: return "int16";
    case da00_type::uint16: return "uint16";
    case da00_type::int32: return "int32";
    case da00_type::uint32: return "uint32";
    case da00_type::int64: return "int64";
    case da00_type::uint64: return "uint64";
    case da00_type::float32: return "float32";
    case da00_type::float64: return "float64";
    case da00_type::c_string: return "c_string";
    default: return "none";
  }
}

DType WriterModule::da00::da00_type_to_dtype(const da00_type & type) {
  switch (type) {
    case da00_type::int8: return DType::int8;
    case da00_type::uint8: return DType::uint8;
    case da00_type::int16: return DType::int16;
    case da00_type::uint16: return DType::uint16;
    case da00_type::int32: return DType::int32;
    case da00_type::uint32: return DType::uint32;
    case da00_type::int64: return DType::int64;
    case da00_type::uint64: return DType::uint64;
    case da00_type::float32: return DType::float32;
    case da00_type::float64: return DType::float64;
    case da00_type::c_string: return DType::c_string;
    default:
      throw std::runtime_error("da00_type_to_dtype: unknown type");
  }
}

da00_type WriterModule::da00::dtype_to_da00_type(const DType & type) {
  switch (type) {
    case DType::int8: return da00_type::int8;
    case DType::uint8: return da00_type::uint8;
    case DType::int16: return da00_type::int16;
    case DType::uint16: return da00_type::uint16;
    case DType::int32: return da00_type::int32;
    case DType::uint32: return da00_type::uint32;
    case DType::int64: return da00_type::int64;
    case DType::uint64: return da00_type::uint64;
    case DType::float32: return da00_type::float32;
    case DType::float64: return da00_type::float64;
    case DType::c_string: return da00_type::c_string;
    default: return da00_type::none;
  }
}
//
//size_t dtype_to_size_t(const DType & type) {
//  return static_cast<size_t>(type);
//}

template<> DType WriterModule::da00::get_dtype(int8_t){return DType::int8;}
template<> DType WriterModule::da00::get_dtype(uint8_t){return DType::uint8;}
template<> DType WriterModule::da00::get_dtype(int16_t){return DType::int16;}
template<> DType WriterModule::da00::get_dtype(uint16_t){return DType::uint16;}
template<> DType WriterModule::da00::get_dtype(int32_t){return DType::int32;}
template<> DType WriterModule::da00::get_dtype(uint32_t){return DType::uint32;}
template<> DType WriterModule::da00::get_dtype(int64_t){return DType::int64;}
template<> DType WriterModule::da00::get_dtype(uint64_t){return DType::uint64;}
template<> DType WriterModule::da00::get_dtype(float){return DType::float32;}
template<> DType WriterModule::da00::get_dtype(double){return DType::float64;}
template<> DType WriterModule::da00::get_dtype(char){return DType::c_string;}