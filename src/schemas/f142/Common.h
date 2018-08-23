#pragma once

#include <flatbuffers/flatbuffers.h>
#include <memory>

namespace FileWriter {
namespace Schemas {
namespace f142 {

template <typename T> using uptr = std::unique_ptr<T>;

#include "schemas/f142_logdata_generated.h"

using FBUF = LogData;
}
}
}
