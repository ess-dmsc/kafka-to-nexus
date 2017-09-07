#pragma once

template <typename T> class SHMP {
public:
  static size_t const null = -1;
  using U = uint8_t *;
  SHMP(void *base, T *p) { off = U(p) - U(base); }
  SHMP(SHMP const &) = delete;
  ~SHMP() { reset(); }
  typename std::add_lvalue_reference<T>::type ref() const {
    return *((T *)(0));
  }
  void reset() {
    if (off != null) {
      // TODO
      // clean up ...
    }
  }
  void swap(SHMP<T> &x, SHMP<T> &y) {
    using std::swap;
    swap(x.off, y.off);
  }

private:
  size_t off = -1;
};

template <typename T> void swap(SHMP<T> &x, SHMP<T> &y) { x.swap(y); }
