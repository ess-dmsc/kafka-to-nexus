#include "WriterUntyped.h"
#include "Exceptions.h"
#include "WriterTyped.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

WriterUntyped::ptr WriterUntyped::createFromJson(json const &Json) {
  // TODO  continue with moving the typed writer to the heap
  throw unimplemented();
}

/// Create the Writer during HDF reopen
WriterUntyped::ptr WriterUntyped::createFromHDF(hdf5::node::Group &Group) {
  throw unimplemented();
}
}
}
}
