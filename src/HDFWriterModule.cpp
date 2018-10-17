#include "HDFWriterModule.h"

namespace FileWriter {

namespace HDFWriterModuleRegistry {

std::map<std::string, HDFWriterModuleRegistry::ModuleFactory> &getFactories() {
  static std::map<std::string, HDFWriterModuleRegistry::ModuleFactory> _items;
  return _items;
}

HDFWriterModuleRegistry::ModuleFactory &find(std::string const &key) {
  static HDFWriterModuleRegistry::ModuleFactory empty;
  auto &_items = getFactories();
  auto f = _items.find(key);
  if (f == _items.end()) {
    return empty;
  }
  return f->second;
}

void addWriterModule(std::string key, ModuleFactory value) {
  auto &m = getFactories();
  if (key.size() != 4) {
    throw std::runtime_error(
        "The number of characters in the Flatbuffer id string must be 4.");
  }
  if (m.find(key) != m.end()) {
    auto s = fmt::format("ERROR entry for key [{}] exists already", key);
    throw std::runtime_error(s);
  }
  m[key] = std::move(value);
}
} // namespace HDFWriterModuleRegistry

// Implementation details for `HDFWriterModule`
namespace HDFWriterModule_detail {

static std::map<int8_t, std::string> const g_InitResult_strings{
    {0, "OK"}, {-1, "ERROR_IO"}, {-2, "ERROR_INCOMPLETE_CONFIGURATION"},
};

std::string InitResult::to_str() const {
  auto const &m = g_InitResult_strings;
  auto const it = m.find(v);
  if (it == m.end()) {
    return "ERROR_UNKNOWN_VALUE";
  }
  return it->second;
}

static std::map<int8_t, std::string> const g_WriteResult_strings{
    {0, "OK"},
    {-1, "ERROR_IO"},
    {-2, "ERROR_BAD_FLATBUFFER"},
    {-3, "ERROR_DATA_STRUCTURE_MISMATCH"},
    {-4, "ERROR_DATA_TYPE_MISMATCH"},
    {-5, "ERROR_WITH_MESSAGE"},
};

std::string WriteResult::to_str() const {
  auto const &m = g_WriteResult_strings;
  auto const it = m.find(v);
  if (it == m.end()) {
    return "ERROR_UNKNOWN_VALUE";
  }
  if (v == -5) {
    return std::string(it->second) + ": " + Message;
  }
  return it->second;
}
} // namespace HDFWriterModule_detail
} // namespace FileWriter
