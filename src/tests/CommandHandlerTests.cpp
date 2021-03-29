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

TEST(ExtractStreamSettings, IfStreamNotDefinedThenThrows) {
  std::string Command{R"""({
    "type": "stream"
  })"""};

  StreamHDFInfo Info;
  Info.ConfigStream = Command;

  ASSERT_THROW(FileWriter::extractStreamInformationFromJsonForSource(Info),
               std::runtime_error);
}

TEST(ExtractStreamSettings, IfSourceNotDefinedThenThrows) {
  std::string Command{R"""({
    "type": "stream",
    "stream": {
        "dtype": "double",
        "writer_module": "f142",
        "topic": "my_test_topic"
    }
  })"""};

  StreamHDFInfo Info;
  Info.ConfigStream = Command;

  ASSERT_THROW(FileWriter::extractStreamInformationFromJsonForSource(Info),
               std::runtime_error);
}

TEST(ExtractStreamSettings, IfTopicNotDefinedThenThrows) {
  std::string Command{R"""({
    "type": "stream",
    "stream": {
        "dtype": "double",
        "writer_module": "f142",
        "source": "my_test_pv"
    }
  })"""};

  StreamHDFInfo Info;
  Info.ConfigStream = Command;

  ASSERT_THROW(FileWriter::extractStreamInformationFromJsonForSource(Info),
               std::runtime_error);
}

TEST(ExtractStreamSettings, IfWriterModuleNotDefinedThenThrows) {
  std::string Command{R"""({
    "type": "stream",
    "stream": {
        "dtype": "double",
        "source": "my_test_pv",
        "topic": "my_test_topic"
    }
  })"""};

  StreamHDFInfo Info;
  Info.ConfigStream = Command;

  ASSERT_THROW(FileWriter::extractStreamInformationFromJsonForSource(Info),
               std::runtime_error);
}

TEST(ExtractStreamSettings, IfValidThenBasicStreamSettingsExtracted) {
  std::string Command{R"""({
    "type": "stream",
    "stream": {
        "dtype": "double",
        "writer_module": "f142",
        "source": "my_test_pv",
        "topic": "my_test_topic"
    }
  })"""};

  StreamHDFInfo Info;
  Info.ConfigStream = Command;

  auto Settings = FileWriter::extractStreamInformationFromJsonForSource(Info);

  ASSERT_EQ("my_test_topic", Settings.Topic);
  ASSERT_EQ("f142", Settings.Module);
  ASSERT_EQ("my_test_pv", Settings.Source);
}

TEST(ExtractStreamSettings, IfAttributesDefinedThenExtracted) {
  std::string Command{R"""({
    "type": "stream",
    "stream": {
        "dtype": "double",
        "writer_module": "f142",
        "source": "my_test_pv",
        "topic": "my_test_topic"
    },
    "attributes": {
          "NX_class": "NXlog"
    }
  })"""};

  StreamHDFInfo Info;
  Info.ConfigStream = Command;

  auto Settings = FileWriter::extractStreamInformationFromJsonForSource(Info);

  ASSERT_EQ("{\"NX_class\":\"NXlog\"}", Settings.Attributes);
}
