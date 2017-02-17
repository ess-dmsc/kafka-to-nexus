#include "synth.h"
#include <random>

namespace BrightnESS {
namespace FlatBufs {
namespace f141_epics_nt {

EpicsPV const * fb::root() {
	return GetEpicsPV(builder->GetBufferPointer());
}

class synth_impl {
friend class synth;
std::mt19937 rnd;
PV type;
};

synth::synth(std::string name, PV type, int size, uint64_t seed) : name(name), size(size) {
	impl.reset(new synth_impl);
	impl->rnd.seed(seed);
	impl->type = type;
}

synth::~synth() {
}

fb synth::next(uint64_t seq) {
	fb ret;
	ret.builder.reset(new flatbuffers::FlatBufferBuilder);
	// NOTE
	// synth does not add fwdinfo because that's, well, for the forwarder to add.
	// we do add timeStamp though, because that's meant to come from the Epics IOC.
	auto n = ret.builder->CreateString(name);
	flatbuffers::Offset<void> pv;
	PV pv_type = PV::NONE;
	switch (impl->type) {
	case PV::NTScalarArrayDouble: {
		std::vector<double> a1;
		a1.push_back(seq);
		for (int i1 = 1; i1 < size; ++i1) {
			a1.push_back((impl->rnd() >> 10) * 1e-3);
		}
		auto d1 = ret.builder->CreateVector(a1);
		NTScalarArrayDoubleBuilder b2(*ret.builder);
		b2.add_value(d1);
		pv_type = PV::NTScalarArrayDouble;
		pv = b2.Finish().Union();
		} break;
	case PV::NTScalarArrayFloat: {
		NTScalarArrayFloatBuilder b2(*ret.builder);
		pv_type = PV::NTScalarArrayFloat;
		pv = b2.Finish().Union();
		} break;
	default:
		break;
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

}
}
}
