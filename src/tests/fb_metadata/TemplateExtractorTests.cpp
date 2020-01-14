// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "fb_metadata_extractors/template/TemplateExtractor.h"
#include "FlatbufferMessage.h"
#include <gtest/gtest.h>

TEST(TemplateTests, ReaderReturnValues) {
  TemplateExtractor::Extractor SomeExtractor;
  EXPECT_TRUE(SomeExtractor.verify(FileWriter::FlatbufferMessage()));
  EXPECT_EQ(SomeExtractor.source_name(FileWriter::FlatbufferMessage()),
            std::string(""));
  EXPECT_EQ(SomeExtractor.timestamp(FileWriter::FlatbufferMessage()), 0u);
}
