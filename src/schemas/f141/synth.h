#pragma once
#include <memory>
#include "schemas/f141_epics_nt_generated.h"
#include <string>

namespace BrightnESS {
namespace FlatBufs {
namespace f141_epics_nt {

class fb {
public:
std::unique_ptr<flatbuffers::FlatBufferBuilder> builder;
EpicsPV const * root();
};

class synth_impl;

class synth {
public:
synth(std::string name, PV type, int size, uint64_t seed);
~synth();
fb next(uint64_t seq);
std::unique_ptr<synth_impl> impl;
std::string name;
int size;
};

}
}
}
