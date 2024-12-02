// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "WriterModule/template/TemplateWriter.h"
#include <gtest/gtest.h>

TEST(TemplateTests, WriterReturnValues) {
  TemplateWriter::WriterClass SomeWriter;
  hdf5::node::Group SomeGroup;
  EXPECT_TRUE(SomeWriter.init_hdf(SomeGroup) == WriterModule::InitResult::OK);
  EXPECT_TRUE(SomeWriter.reopen(SomeGroup) == WriterModule::InitResult::OK);
  EXPECT_NO_THROW(SomeWriter.write(FileWriter::FlatbufferMessage(), false));
}
