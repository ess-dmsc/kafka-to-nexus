// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "TemplateWriter.h"

namespace TemplateWriter {

// Creates a factory function used to instantiate zero or more WriterClass, i.e.
// one for every data source which produces data with the file id "test".
static Module::Registrar<WriterClass>
    RegisterWriter("test");

} // namespace TemplateWriter
