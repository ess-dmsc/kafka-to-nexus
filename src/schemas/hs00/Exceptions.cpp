#include "Exceptions.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

std::runtime_error unimplemented() {
  return std::runtime_error("unimplemented");
}
}
}
}
