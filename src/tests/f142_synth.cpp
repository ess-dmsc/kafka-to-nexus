#include "f142_synth.h"
#include <random>

namespace FlatBufs {
namespace f142 {

LogData const *FlatBufferWrapper::root() {
  return GetLogData(builder->GetBufferPointer());
}

Synth::Synth(std::string SynthName, Value Type)
    : Name(std::move(SynthName)), Impl(std::make_unique<SynthImpl>()) {
  Impl->Type = Type;
}

FlatBufferWrapper Synth::next(uint64_t const TestValue,
                              size_t const NrOfElements) {
  FlatBufferWrapper ret;
  ret.builder = std::make_unique<flatbuffers::FlatBufferBuilder>();
  // NOTE
  // Synth does not add fwdinfo because that's, well, for the forwarder to add.
  // we do add timeStamp though, because that's meant to come from the Epics
  // IOC.
  auto n = ret.builder->CreateString(Name);
  flatbuffers::Offset<void> value;
  Value value_type = Value::NONE;
  // TODO make general..
  switch (Impl->Type) {
  case Value::ArrayInt: {
    using T = int32_t;
    T *BufferPtr = nullptr;
    auto d1 = ret.builder->CreateUninitializedVector(
        NrOfElements, sizeof(T), reinterpret_cast<uint8_t **>(&BufferPtr));
    for (size_t i1 = 0; i1 < NrOfElements; ++i1) {
      BufferPtr[i1] = TestValue;
    }
    ArrayIntBuilder b2(*ret.builder);
    b2.add_value(d1);
    value_type = Value::ArrayInt;
    value = b2.Finish().Union();
  } break;
  case Value::ArrayDouble: {
    using T = double;
    T *BufferPtr = nullptr;
    auto d1 = ret.builder->CreateUninitializedVector(
        NrOfElements, sizeof(T), reinterpret_cast<uint8_t **>(&BufferPtr));
    for (size_t i1 = 0; i1 < NrOfElements; ++i1) {
      BufferPtr[i1] = TestValue;
    }
    ArrayDoubleBuilder b2(*ret.builder);
    b2.add_value(d1);
    value_type = Value::ArrayDouble;
    value = b2.Finish().Union();
  } break;
  case Value::ArrayFloat: {
    using T = float;
    T *BufferPtr = nullptr;
    auto d1 = ret.builder->CreateUninitializedVector(
        NrOfElements, sizeof(T), reinterpret_cast<uint8_t **>(&BufferPtr));
    for (size_t i1 = 0; i1 < NrOfElements; ++i1) {
      BufferPtr[i1] = TestValue;
    }
    ArrayFloatBuilder b2(*ret.builder);
    b2.add_value(d1);
    value_type = Value::ArrayFloat;
    value = b2.Finish().Union();
  } break;
  case Value::Double: {
    DoubleBuilder b2(*ret.builder);
    b2.add_value(TestValue);
    value_type = Impl->Type;
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
