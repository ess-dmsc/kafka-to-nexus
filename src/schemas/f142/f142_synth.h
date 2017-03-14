#pragma once
#include <memory>
#include "schemas/f142_logdata_generated.h"
#include <string>

namespace BrightnESS {
namespace FlatBufs {
namespace f142 {

class fb {
public:
std::unique_ptr<flatbuffers::FlatBufferBuilder> builder;
LogData const * root();
};

class synth_impl;

class synth {
public:
synth(std::string name, Value type, int size);
~synth();
fb next(uint64_t seq);
std::unique_ptr<synth_impl> impl;
std::string name;
int size;
};

}
}
}
