#include "HDFWriterModule.h"

namespace FileWriter {

namespace HDFWriterModuleRegistry {

std::map<std::string, HDFWriterModuleRegistry::ModuleFactory> &getFactories() {
  static std::map<std::string, HDFWriterModuleRegistry::ModuleFactory> _items;
  return _items;
}

HDFWriterModuleRegistry::ModuleFactory &find(std::string const &key) {
  auto &_items = getFactories();
  return _items.at(key);
}

void addWriterModule(std::string const &Key, ModuleFactory Value) {
  auto &m = getFactories();
  if (Key.size() != 4) {
    throw std::runtime_error(
        "The number of characters in the Flatbuffer id string must be 4.");
  }
  if (m.find(Key) != m.end()) {
    auto s = fmt::format("ERROR entry for key [{}] exists already", Key);
    throw std::runtime_error(s);
  }
  m[Key] = std::move(Value);
}
} // namespace HDFWriterModuleRegistry

// Implementation details for `HDFWriterModule`
namespace HDFWriterModule_detail {

static std::map<int8_t, std::string> const InitResultStrings{
    {0, "OK"}, {-1, "ERROR_IO"}, {-2, "ERROR_INCOMPLETE_CONFIGURATION"},
};

static std::map<int8_t, std::string> const WriteResultStrings{
    {0, "OK"},
    {-1, "ERROR_IO"},
    {-2, "ERROR_BAD_FLATBUFFER"},
    {-3, "ERROR_DATA_STRUCTURE_MISMATCH"},
    {-4, "ERROR_DATA_TYPE_MISMATCH"},
    {-5, "ERROR_WITH_MESSAGE"},
};

std::string WriteResult::to_str() const {
  auto const &m = WriteResultStrings;
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
