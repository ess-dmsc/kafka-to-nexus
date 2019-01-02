#include "f142_synth.h"
#include <random>

namespace FlatBufs {
namespace f142 {

LogData const *fb::root() { return GetLogData(builder->GetBufferPointer()); }

class synth_impl {
  friend class synth;
  std::mt19937 rnd;
  Value type;
};

  synth::synth(std::string SynthName, Value Type) : Name(std::move(SynthName)) {
  impl.reset(new synth_impl);
  impl->type = Type;
}

synth::~synth() {}

fb synth::next(uint64_t seq, size_t nele) {
  impl->rnd.seed(seq);
  fb ret;
  ret.builder.reset(new flatbuffers::FlatBufferBuilder);
  // NOTE
  // synth does not add fwdinfo because that's, well, for the forwarder to add.
  // we do add timeStamp though, because that's meant to come from the Epics
  // IOC.
  auto n = ret.builder->CreateString(Name);
  flatbuffers::Offset<void> value;
  Value value_type = Value::NONE;
  // TODO make general..
  switch (impl->type) {
  case Value::ArrayInt: {
    using T = int32_t;
    T *a1 = nullptr;
    auto d1 = ret.builder->CreateUninitializedVector(nele, sizeof(T),
                                                     (uint8_t **)&a1);
    for (size_t i1 = 0; i1 < nele; ++i1) {
      // a1[i1] = impl->rnd() >> 25;
      a1[i1] = seq;
    }
    ArrayIntBuilder b2(*ret.builder);
    b2.add_value(d1);
    value_type = Value::ArrayInt;
    value = b2.Finish().Union();
  } break;
  case Value::ArrayDouble: {
    using T = double;
    T *a1 = nullptr;
    auto d1 = ret.builder->CreateUninitializedVector(nele, sizeof(T),
                                                     (uint8_t **)&a1);
    for (size_t i1 = 0; i1 < nele; ++i1) {
      // a1[i1] = impl->rnd() >> 25;
      a1[i1] = seq;
    }
    ArrayDoubleBuilder b2(*ret.builder);
    b2.add_value(d1);
    value_type = Value::ArrayDouble;
    value = b2.Finish().Union();
  } break;
  case Value::ArrayFloat: {
    using T = float;
    T *a1 = nullptr;
    auto d1 = ret.builder->CreateUninitializedVector(nele, sizeof(T),
                                                     (uint8_t **)&a1);
    for (size_t i1 = 0; i1 < nele; ++i1) {
      // a1[i1] = impl->rnd() >> 25;
      a1[i1] = seq;
    }
    ArrayFloatBuilder b2(*ret.builder);
    b2.add_value(d1);
    value_type = Value::ArrayFloat;
    value = b2.Finish().Union();
  } break;
  case Value::Double: {
    DoubleBuilder b2(*ret.builder);
    b2.add_value(seq);
    value_type = impl->type;
    value = b2.Finish().Union();
  } break;
  default:
    break;
  }
  LogDataBuilder b1(*ret.builder);
  b1.add_timestamp(123123123);
  b1.add_source_name(n);
  b1.add_value_type(value_type);
  b1.add_value(value);
  FinishLogDataBuffer(*ret.builder, b1.Finish());
  return ret;
}

} // namespace f142
} // namespace FlatBufs
