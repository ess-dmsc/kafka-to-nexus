// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "JobCreator.h"
#include <gtest/gtest.h>

TEST(ExtractStreamSettings, IfSourceNotDefinedThenThrows) {
  std::string Command{R"""({
        "dtype": "double",
        "topic": "my_test_topic"
  })"""};

  ModuleHDFInfo Info;
  Info.WriterModule = "f142";
  Info.ConfigStream = Command;

  ASSERT_THROW(FileWriter::extractModuleInformationFromJsonForSource(Info),
               std::runtime_error);
}

TEST(ExtractStreamSettings, IfTopicNotDefinedThenThrows) {
  std::string Command{R"""({
        "dtype": "double",
        "source": "my_test_pv"
  })"""};

  ModuleHDFInfo Info;
  Info.WriterModule = "f142";
  Info.ConfigStream = Command;

  ASSERT_THROW(FileWriter::extractModuleInformationFromJsonForSource(Info),
               std::runtime_error);
}

TEST(ExtractStreamSettings, IfWriterModuleNotDefinedThenThrows) {
  std::string Command{R"""({
        "dtype": "double",
        "source": "my_test_pv",
        "topic": "my_test_topic"
  })"""};

  ModuleHDFInfo Info;
  Info.ConfigStream = Command;

  ASSERT_THROW(FileWriter::extractModuleInformationFromJsonForSource(Info),
               std::runtime_error);
}

TEST(ExtractStreamSettings, IfValidThenBasicStreamSettingsExtracted) {
  std::string Command{R"""({
        "dtype": "double",
        "source": "my_test_pv",
        "topic": "my_test_topic"
  })"""};

  ModuleHDFInfo Info;
  Info.WriterModule = "f142";
  Info.ConfigStream = Command;

  auto Settings = FileWriter::extractModuleInformationFromJsonForSource(Info);

  ASSERT_EQ("my_test_topic", Settings.Topic);
  ASSERT_EQ("f142", Settings.Module);
  ASSERT_EQ("my_test_pv", Settings.Source);
}

TEST(ExtractStreamSettings, IfAttributesDefinedThenExtracted) {
  std::string Command{R"""({
        "dtype": "double",
        "source": "my_test_pv",
        "topic": "my_test_topic",
    "attributes": {
          "NX_class": "NXlog"
    }
  })"""};

  ModuleHDFInfo Info;
  Info.WriterModule = "f142";
  Info.ConfigStream = Command;

  auto Settings = FileWriter::extractModuleInformationFromJsonForSource(Info);

  ASSERT_EQ(R"({"NX_class":"NXlog"})", Settings.Attributes);
}
