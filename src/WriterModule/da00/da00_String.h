#pragma once
#include "HDFFile.h"
#include "logger.h"
#include <fmt/core.h>
#include <h5cpp/hdf5.hpp>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <vector>

namespace WriterModule::da00 {

class StringConfig {
public:
  StringConfig() = default;
  StringConfig &operator=(StringConfig const &other) = default;
  StringConfig(StringConfig const &other) = default;
  StringConfig(StringConfig &&other) noexcept = default;

  explicit StringConfig(std::string const &value) : _value(value) {}
  explicit StringConfig(std::string &&value) : _value(std::move(value)) {}
  explicit StringConfig(ssize_t size) : _size(size) {}
  explicit StringConfig(nlohmann::json const &config) {
    if (config.is_string()) {
      _value = config.get<std::string>();
    } else if (config.is_number_integer()) {
      _size = config.get<ssize_t>();
    } else if (config.is_object()) {
      if (config.contains("value"))
        _value = config["value"].get<std::string>();
      if (config.contains("size"))
        _size = config["size"].get<ssize_t>();
    } else {
      LOG_ERROR("Invalid StringConfig JSON input {}", config.dump());
      throw std::runtime_error("Invalid StringConfig JSON input");
    }
  }

  bool operator==(const std::string &other) const {
    return has_value() && value() == other;
  }

  [[nodiscard]] bool has_value() const { return _value.has_value(); }
  [[nodiscard]] bool has_size() const { return _size.has_value(); }
  [[nodiscard]] ssize_t size() const { return _size.value(); }
  [[nodiscard]] std::string const &value() const { return _value.value(); }
  [[nodiscard]] bool knows_value() const { return has_value() || has_size(); }

  template <class ParentUniquePointer>
  hdf5::attribute::Attribute insert_attribute(ParentUniquePointer &parent,
                                              std::string const &name) const {
    if (!knows_value()) {
      auto pos = std::string(parent->link().path());
      auto msg = fmt::format(
          "StringConfig can not create attribute {}@{} without value or size",
          pos, name);
      LOG_ERROR(msg);
      throw std::runtime_error(msg);
    }
    auto s = has_size() ? size() : 0u;
    auto v = has_value() ? static_cast<ssize_t>(value().size()) : 0u;
    auto output = hdf5::datatype::String::fixed(v > s ? v : s);
    auto attr =
        parent->attributes.create(name, output, hdf5::dataspace::Scalar());
    if (has_value()) {
      try {
        attr.write(value(), output);
      } catch (std::exception &e) {
        LOG_ERROR("Failed to write attribute {}@{}: {}",
                  std::string(parent->link().path()), name, e.what());
      }
    }
    return attr;
  }

  std::pair<bool, bool> update_from(const StringConfig &other, bool force) {
    bool inconsistent{false}, changed{false};
    if (other.has_value()) {
      if (has_value() && value() != other.value() && force) {
        LOG_DEBUG("Unexpected StringConfig update: {} -> {}", *this, other);
        _value = other.value();
        if (static_cast<ssize_t>(other.value().size()) >
            std::max(static_cast<ssize_t>(value().size()), size())) {
          inconsistent = true;
        }
        changed = true;
      } else if (has_size() &&
                 size() >= static_cast<ssize_t>(other.value().size())) {
        _value = other.value();
        changed = true;
      } else {
        LOG_ERROR("Update StringConfig value too large: {} -> {}", *this,
                  other);
      }
    }
    return std::make_pair(inconsistent, changed);
  }
private:
  std::optional<std::string> _value;
  std::optional<ssize_t> _size;
};

} // namespace WriterModule::da00

template <> struct fmt::formatter<WriterModule::da00::StringConfig> {
  template <class ParseContext> constexpr auto parse(ParseContext &ctx) {
    return ctx.begin();
  }
  template <class FormatContext>
  auto format(const WriterModule::da00::StringConfig &c, FormatContext &ctx) {
    std::stringstream ss;
    ss << "StringConfig";
    if (c.has_value() && c.has_size()) {
      auto s = c.size();
      auto v = static_cast<ssize_t>(c.value().size());
      ss << "[" << (s > v ? s : v) << "]"
         << "(" << c.value() << ")";
    } else if (c.has_value()) {
      ss << "[" << c.value().size() << "](" << c.value() << ")";
    } else if (c.has_size()) {
      ss << "[" << c.size() << "]()";
    } else {
      ss << "[]()";
    }
    return format_to(ctx.out(), ss.str());
  }
};
