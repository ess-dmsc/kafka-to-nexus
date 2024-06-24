#pragma once
#include <fmt/core.h>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <vector>

namespace WriterModule::da00 {

class EdgeConfig {
  std::optional<nlohmann::json> _edges;
  std::optional<nlohmann::json> _first;
  std::optional<nlohmann::json> _last;
  std::optional<ssize_t> _size;

public:
  EdgeConfig() = default;
  EdgeConfig &operator=(std::string const &config) {
    auto json = nlohmann::json::parse(config);
    if (json.is_array())
      _edges = json;
    else if (json.is_object()) {
      if (json.contains("first"))
        _first = json["first"];
      if (json.contains("last"))
        _last = json["last"];
      if (json.contains("size"))
        _size = json["size"];
    }
    if (_edges.has_value() ||
        (_first.has_value() && _last.has_value() && _size.has_value()))
      return *this;
    LOG_ERROR("Invalid EdgeConfig JSON input {} -> edges = {}, first = {}, "
              "last = {}, size = {}",
              config, _edges.has_value(), _first.has_value(), _last.has_value(),
              _size.has_value());
    throw std::runtime_error("Invalid EdgeConfig JSON input");
  }
  EdgeConfig &operator=(EdgeConfig const &other) = default;
  EdgeConfig(EdgeConfig const &other) = default;
  EdgeConfig(EdgeConfig &&other) noexcept = default;

  [[nodiscard]] bool has_edges() const { return _edges.has_value(); }
  [[nodiscard]] bool has_first() const { return _first.has_value(); }
  [[nodiscard]] bool has_last() const { return _last.has_value(); }
  [[nodiscard]] bool has_size() const { return _size.has_value(); }
  [[nodiscard]] ssize_t size() const { return _size.value(); }
  [[nodiscard]] nlohmann::json const &edges_json() const {
    return _edges.value();
  }
  [[nodiscard]] nlohmann::json const &first_json() const {
    return _first.value();
  }
  [[nodiscard]] nlohmann::json const &last_json() const {
    return _last.value();
  }
  [[nodiscard]] bool knows_edges() const {
    return has_edges() || (has_first() && has_last() && has_size());
  }
  template <class T> [[nodiscard]] std::vector<T> edges() const {
    std::vector<T> out;
    if (_edges.has_value())
      out = _edges.value().get<std::vector<T>>();
    if (out.size() < 2 && _first.has_value() && _last.has_value() &&
        _size.has_value()) {
      T first = _first.value().get<T>();
      T last = _last.value().get<T>();
      ssize_t size = _size.value();
      if (size < 2) {
        throw std::runtime_error("Edges require size > 1");
      }
      T step = (last - first) / static_cast<T>(size - 1);
      out.resize(size);
      for (ssize_t i = 0; i < size; ++i)
        out[i] = first + static_cast<T>(i) * step;
    }
    return out;
  }
};
} // namespace WriterModule::da00

template <> struct fmt::formatter<WriterModule::da00::EdgeConfig> {
  template <typename ParseContext> constexpr auto parse(ParseContext &ctx) {
    return ctx.begin();
  }
  template <typename FormatContext>
  auto format(const WriterModule::da00::EdgeConfig &edge, FormatContext &ctx) {
    const auto values =
        edge.has_edges() ? edge.edges_json() : nlohmann::json::array();
    const auto first = edge.has_first() ? edge.first_json() : nlohmann::json();
    const auto last = edge.has_last() ? edge.last_json() : nlohmann::json();
    const auto size = edge.has_size() ? edge.size() : -1;
    return format_to(ctx.out(),
                     "EdgeConfig(edge={}, first={}, last={}, size={})",
                     values.dump(), first.dump(), last.dump(), size);
  }
};
