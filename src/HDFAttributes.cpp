// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se


#include "HDFAttributes.h"

namespace HDFAttributes {

void writeAttribute(hdf5::node::Node const &Node,
                    const std::string &Name, time_point Value) {
  writeAttribute(Node, Name, toUTCDateTime(Value));
}

} // namespace HDFAttributes
