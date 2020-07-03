// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FileWriterTask.h"
#include "Source.h"
#include <gtest/gtest.h>

TEST(FileWriterTask, WithPrefixFullFileNameIsCorrect) {
  FileWriter::FileWriterTask Task("SomeID");

  Task.setFilename("SomePrefix", "File.hdf");

  ASSERT_EQ("SomePrefix/File.hdf", Task.filename());
}

TEST(FileWriterTask, WithoutPrefixFileNameIsCorrect) {
  FileWriter::FileWriterTask Task("SomeID");

  Task.setFilename("", "File.hdf");

  ASSERT_EQ("File.hdf", Task.filename());
}

TEST(FileWriterTask, AddingSourceAddsToDemuxers) {
  FileWriter::FileWriterTask Task("SomeID");
  FileWriter::Source Src("Src1", "Id1", "Id2", "Topic1", nullptr);

  Task.addSource(std::move(Src));

  ASSERT_EQ(1u, Task.sources().size());
}

TEST(FileWriterTask, SettingJobIdSetsID) {
  FileWriter::FileWriterTask Task("SomeID");
  std::string NewId = "NewID";

  Task.setJobId(NewId);

  ASSERT_EQ(NewId, Task.jobID());
}
