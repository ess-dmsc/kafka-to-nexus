#pragma once
#include <memory>
#include "schemas/ev42_events_generated.h"
#include <string>

namespace BrightnESS {
namespace FlatBufs {
namespace ev42 {

class fb {
public:
std::unique_ptr<flatbuffers::FlatBufferBuilder> builder;
EventMessage const * root();
};

class synth_impl;

class synth {
public:
synth(std::string name, int size, uint64_t seed);
~synth();
fb next();
std::unique_ptr<synth_impl> impl;
std::string name;
int size;
};

}
}
}
