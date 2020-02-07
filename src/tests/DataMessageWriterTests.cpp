// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "DataMessageWriter.h"
#include "WriterModuleBase.h"
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

class DataMessageWriterTest : public ::testing::Test {

};

class WriterModuleStandIn : WriterModule::Base {
public:
  MAKE_MOCK1(parse_config, void(std::string const&), override);
  MAKE_MOCK2(init_hdf, WriterModule::InitResult(hdf5::node::Group&, std::string const&), override);
  MAKE_MOCK1(reopen, WriterModule::InitResult(hdf5::node::Group&), override);
  MAKE_MOCK1(write, void(FileWriter::FlatbufferMessage const&), override);
};

TEST_F(DataMessageWriterTest, WriteMessageSuccess) {

}
