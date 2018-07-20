#include "Exceptions.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

std::runtime_error unimplemented() {
  exit(200);
  return std::runtime_error("unimplemented");
}
}
}
}
