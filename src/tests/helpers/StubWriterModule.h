// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include "FlatbufferMessage.h"
#include "WriterModuleBase.h"

using Module::InitResult;

class StubWriterModule : public Module::WriterBase {
public:
  void parse_config(std::string const & /*ConfigurationStream*/) override {}
  InitResult init_hdf(hdf5::node::Group & /*HDFGroup*/,
                      std::string const & /*HDFAttributes*/) override {
    return InitResult::OK;
  }
  InitResult reopen(hdf5::node::Group & /*HDFGroup*/) override {
    return InitResult::OK;
  }
  void write(FileWriter::FlatbufferMessage const & /*Message*/) override {}
};
