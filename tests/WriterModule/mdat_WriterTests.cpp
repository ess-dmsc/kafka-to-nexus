// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ModuleHDFInfo.h"
#include "WriterModule/mdat/mdat_Writer.h"
#include <gtest/gtest.h>

TEST(mdatWriterTests, IgnoresSettingStartTimeIfNotInNotDefined) {
  std::vector<ModuleHDFInfo> MdatModules = {
      {"mdat", "/entry", "{\"name\":\"end_time\"}"}};
  WriterModule::mdat::mdat_Writer Writer;
  Writer.defineMetadata(MdatModules);

  EXPECT_NO_THROW(Writer.setStartTime(time_point{123456ms}));
}

TEST(mdatWriterTests, IgnoresSettingEndTimeIfNotInNotDefined) {
  std::vector<ModuleHDFInfo> MdatModules = {
      {"mdat", "/entry", "{\"name\":\"start_time\"}"}};
  WriterModule::mdat::mdat_Writer Writer;
  Writer.defineMetadata(MdatModules);

  EXPECT_NO_THROW(Writer.setStopTime(time_point{123456ms}));
}
