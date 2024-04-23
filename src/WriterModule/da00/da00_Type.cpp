#include "da00_Type.h"

using namespace WriterModule::da00;

da00_dtype WriterModule::da00::string_to_da00_dtype(const std::string & name) {
  if (name == "none") return da00_dtype::none;
  if (name == "int8") return da00_dtype::int8;
  if (name == "uint8") return da00_dtype::uint8;
  if (name == "int16") return da00_dtype::int16;
  if (name == "uint16") return da00_dtype::uint16;
  if (name == "int32") return da00_dtype::int32;
  if (name == "uint32") return da00_dtype::uint32;
  if (name == "int64") return da00_dtype::int64;
  if (name == "uint64") return da00_dtype::uint64;
  if (name == "float32") return da00_dtype::float32;
  if (name == "float64") return da00_dtype::float64;
  if (name == "c_string") return da00_dtype::c_string;
  return da00_dtype::none;
}

const char * WriterModule::da00::da00_dtype_to_string(const da00_dtype &type) {
  switch (type) {
  case da00_dtype::int8: return "int8";
  case da00_dtype::uint8: return "uint8";
  case da00_dtype::int16: return "int16";
  case da00_dtype::uint16: return "uint16";
  case da00_dtype::int32: return "int32";
  case da00_dtype::uint32: return "uint32";
  case da00_dtype::int64: return "int64";
  case da00_dtype::uint64: return "uint64";
  case da00_dtype::float32: return "float32";
  case da00_dtype::float64: return "float64";
  case da00_dtype::c_string: return "c_string";
  default: return "none";
  }
}

da00_dtype WriterModule::da00::guess_dtype(const nlohmann::json & data){
  if (data.is_object()){
    if (data.contains("first") && data.contains("last") && data.contains("size")){
      if (data["first"].is_number_float() || data["last"].is_number_float())
        return da00_dtype::float64;
      // with integer end points we need to see if the step size is integer
      auto f_int = data["first"].get<std::int64_t>();
      auto l_int = data["last"].get<std::int64_t>();
      auto s_int = data["size"].get<std::int64_t>();
      if (s_int < 1) return da00_dtype::none;
      std::int64_t step = (l_int - f_int) / (s_int - 1);
      if (step == 0) return da00_dtype::float64;
      if ((l_int - f_int) % step == 0) return da00_dtype::int64;
      return da00_dtype::float64;
    }
  }
  if (data.is_null()) return da00_dtype::none;
  if (data.is_boolean()) return da00_dtype::uint8;
  if (data.is_number_integer()) return da00_dtype::int64;
  if (data.is_number_unsigned()) return da00_dtype::uint64;
  if (data.is_number_float()) return da00_dtype::float64;
  if (data.is_string()) return da00_dtype::c_string;
  if (data.is_array()) {
    if (data.empty()) return da00_dtype::none;
    if (data[0].is_boolean()) return da00_dtype::uint8;
    if (data[0].is_number_integer()) return da00_dtype::int64;
    if (data[0].is_number_unsigned()) return da00_dtype::uint64;
    if (data[0].is_number_float()) return da00_dtype::float64;
    if (data[0].is_string()) return da00_dtype::c_string;
  }
  return da00_dtype::none;
}

std::vector<hsize_t> WriterModule::da00::get_shape(const nlohmann::json & data){
  // return an empty array if the data is scalar,
  // otherwise an array (which we don't know the shape of) is a vector
  //  if (data.is_object() && data.contains("shape")) {
  //    auto shape = data["shape"];
  //    if (shape.is_array()) {
  //      std::vector<hsize_t> result;
  //      for (auto &element : shape) {
  //        if (!element.is_number_unsigned()) return {};
  //        result.push_back(element.get<hsize_t>());
  //      }
  //      return result;
  //    }
  //  }
  if (data.is_object() && data.contains("size")){
    return {data["size"].get<hsize_t>()};
  }
  if (!data.is_array() || data.empty()) return {};
  return {data.size()};
}

template<> da00_dtype WriterModule::da00::get_dtype(int8_t){return da00_dtype::int8;}
template<> da00_dtype WriterModule::da00::get_dtype(uint8_t){return da00_dtype::uint8;}
template<> da00_dtype WriterModule::da00::get_dtype(int16_t){return da00_dtype::int16;}
template<> da00_dtype WriterModule::da00::get_dtype(uint16_t){return da00_dtype::uint16;}
template<> da00_dtype WriterModule::da00::get_dtype(int32_t){return da00_dtype::int32;}
template<> da00_dtype WriterModule::da00::get_dtype(uint32_t){return da00_dtype::uint32;}
template<> da00_dtype WriterModule::da00::get_dtype(int64_t){return da00_dtype::int64;}
template<> da00_dtype WriterModule::da00::get_dtype(uint64_t){return da00_dtype::uint64;}
template<> da00_dtype WriterModule::da00::get_dtype(float){return da00_dtype::float32;}
template<> da00_dtype WriterModule::da00::get_dtype(double){return da00_dtype::float64;}
template<> da00_dtype WriterModule::da00::get_dtype(char){return da00_dtype::c_string;}