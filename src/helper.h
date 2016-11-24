#pragma once
#include <memory>
#include <utility>

// note: this implementation does not disable this overload for array types
template <typename T, typename... TX>
std::unique_ptr<T> make_unique(TX && ... tx) {
	return std::unique_ptr<T>(new T(std::forward<TX>(tx)...));
}
