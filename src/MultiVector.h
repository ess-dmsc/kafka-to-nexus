// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <h5cpp/dataspace/simple.hpp>
#include <h5cpp/hdf5.hpp>
#include <numeric>
#include <vector>

using Shape = std::vector<size_t>;

inline size_t extentToSize(Shape S) {
  return std::accumulate(std::next(S.begin()), S.end(), S.at(0),
                         [](auto a, auto b) { return a * b; });
}

inline size_t posToIndex(Shape Dimensions, Shape Pos) {
  size_t ReturnIndex = 0;
  std::reverse(Dimensions.begin(), Dimensions.end());
  std::reverse(Pos.begin(), Pos.end());
  for (size_t i = 0; i < Pos.size(); i++) {
    ReturnIndex += std::accumulate(Dimensions.begin(), Dimensions.begin() + i,
                                   1, std::multiplies<size_t>()) *
                   Pos[i];
  }
  return ReturnIndex;
}

/// \brief Template for representing multi dimensional arrays of data.
template <typename T> class MultiVector {
public:
  MultiVector() = default;
  explicit MultiVector(Shape const &extent)
      : Data(extentToSize(extent)), _shape(extent) {}

  bool operator==(MultiVector<T> const &other) const {
    return _shape == other._shape &&
           std::equal(Data.cbegin(), Data.cend(), other.Data.cbegin());
  }

  void set_value(Shape const &index, T const &value) {
    check_index(index);
    Data[(posToIndex(_shape, index))] = value;
  }

  T get_value(Shape const &index) {
    check_index(index);
    return Data[(posToIndex(_shape, index))];
  }
  [[nodiscard]] Shape getDimensions() const { return _shape; }
  T *data() { return Data.data(); }
  [[nodiscard]] size_t size() const { return Data.size(); }
  std::vector<T> Data;

private:
  void check_index(Shape const &index) {
    if (index.size() != _shape.size()) {
      throw std::out_of_range("Shape dimensions are not equal.");
    }
    for (size_t i = 0; i < _shape.size(); ++i) {
      if (index[i] >= _shape[i]) {
        throw std::out_of_range("Outside of range.");
      }
    }
  }

  Shape _shape;
};

namespace hdf5 {
namespace datatype {
/// Required for h5cpp to write data provided using ArrayAdapter.
template <typename T> class TypeTrait<MultiVector<T>> {
public:
  using Type = MultiVector<T>;
  using TypeClass = typename TypeTrait<T>::TypeClass;
  static TypeClass create(const Type & = Type()) {
    return TypeTrait<typename std::remove_const<T>::type>::create();
  }
  const static TypeClass &get(const Type & = Type()) {
    const static TypeClass &cref = create();
    return cref;
  }
};

template <> class TypeTrait<MultiVector<std::string>> {
public:
  using Type = MultiVector<std::string>;
  using TypeClass = typename TypeTrait<std::string>::TypeClass;
  static TypeClass create(const Type & = Type()) {
    auto string_type = hdf5::datatype::String::variable();
    string_type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    string_type.padding(hdf5::datatype::StringPad::NullTerm);
    return string_type;
  }
};
} // namespace datatype
namespace dataspace {
// Required for h5cpp to write data provided using ArrayAdapter.
template <typename T> class TypeTrait<MultiVector<T>> {
public:
  using DataspaceType = Simple;

  static DataspaceType create(const MultiVector<T> &value) {
    auto Dims = value.getDimensions();
    return Simple(Dimensions(Dims.begin(), Dims.end()),
                  Dimensions(Dims.begin(), Dims.end()));
  }

  static const Dataspace &get(const MultiVector<T> &value,
                              DataspacePool &pool) {
    auto Dims = value.getDimensions();
    return pool.getSimple(Dimensions(Dims.begin(), Dims.end()),
                          Dimensions(Dims.begin(), Dims.end()));
  }

  static void *ptr(MultiVector<T> &data) {
    return reinterpret_cast<void *>(data.Data.data());
  }

  static const void *cptr(const MultiVector<T> &data) {
    return reinterpret_cast<const void *>(data.Data.data());
  }
};
} // namespace dataspace

template <> struct VarLengthStringTrait<MultiVector<std::string>> {
  using BufferType = VarLengthStringBuffer<char>;
  using DataType = std::vector<std::string>;

  static BufferType to_buffer(const DataType &data) {
    BufferType buffer;
    std::transform(
        data.begin(), data.end(), std::back_inserter(buffer),
        [](const std::string &str) { return const_cast<char *>(str.c_str()); });
    return buffer;
  }

  static void from_buffer(const BufferType &buffer, DataType &data) {
    std::transform(
        buffer.begin(), buffer.end(), data.begin(),
        [](const char *ptr) { return std::string(ptr, std::strlen(ptr)); });
  }
};
} // namespace hdf5
