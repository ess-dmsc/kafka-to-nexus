#pragma once
#include "schemas/f141_epics_nt_generated.h"
#include <memory>
#include <random>
#include <string>
#include <type_traits>

namespace FlatBufs {
namespace f141_epics_nt {

using namespace BrightnESS::FlatBufs;
using namespace BrightnESS::FlatBufs::f141_epics_nt;
using _V = PV;
struct nt_val_type_U {
  using _V = PV;
};

template <typename T> struct nt_val_type : public nt_val_type_U {
  static _V v();
};

template <> struct nt_val_type<int32_t> {
  static _V v() { return _V::NTScalarArrayInt; }
};
template <> struct nt_val_type<double> {
  static _V v() { return _V::NTScalarArrayDouble; }
};
template <> struct nt_val_type<float> {
  static _V v() { return _V::NTScalarArrayFloat; }
};

class fb {
public:
  std::unique_ptr<flatbuffers::FlatBufferBuilder> builder;
  EpicsPV const *root();
};

class synth_impl {
  friend class synth;
  std::mt19937 rnd;
  PV type;
};

class synth {
public:
  synth(std::string name, PV type, int size, uint64_t seed);
  ~synth();

  template <typename T> fb next(uint64_t seq) {
    using Tb = typename std::conditional<
        std::is_same<T, int32_t>::value, NTScalarArrayIntBuilder,
        typename std::conditional<std::is_same<T, double>::value,
                                  NTScalarArrayDoubleBuilder,
                                  std::nullptr_t>::type>::type;

    // impl->rnd.seed(seq);

    fb ret;
    ret.builder.reset(new flatbuffers::FlatBufferBuilder);
    // NOTE
    // synth does not add fwdinfo because that's, well, for the forwarder to
    // add. we do add timeStamp though, because that's meant to come from the
    // Epics IOC.
    auto n = ret.builder->CreateString(name);
    flatbuffers::Offset<void> pv;
    PV pv_type = PV::NONE;
    if (impl->type != nt_val_type<T>::v()) {
      printf("error\n");
    } else {
      T *a1 = nullptr;
      auto v1 = ret.builder->CreateUninitializedVector(size, sizeof(T),
                                                       (uint8_t **)&a1);
      for (int i1 = 0; i1 < size; ++i1) {
        // a1[i1] = impl->rnd() >> 25;
        a1[i1] = seq;
      }
      Tb b2(*ret.builder);
      b2.add_value(v1);
      pv_type = impl->type;
      pv = b2.Finish().Union();
    }
    auto ts = timeStamp_t(123, 456);
    EpicsPVBuilder b1(*ret.builder);
    b1.add_timeStamp(&ts);
    b1.add_name(n);
    b1.add_pv_type(pv_type);
    b1.add_pv(pv);
    FinishEpicsPVBuffer(*ret.builder, b1.Finish());
    return ret;
  }

  std::unique_ptr<synth_impl> impl;
  std::string name;
  int size;
};

} // namespace f141_epics_nt
} // namespace FlatBufs
