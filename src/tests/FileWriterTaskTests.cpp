// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FileWriterTask.h"
#include "Metrics/MockSink.h"
#include "Source.h"
#include <gtest/gtest.h>

class FileWriterTask : public ::testing::Test {
public:
  std::unique_ptr<Metrics::Sink> TestSink{new Metrics::MockSink()};
  std::shared_ptr<Metrics::Reporter> TestReporter{
      new Metrics::Reporter(std::move(TestSink), 10ms)};
  std::vector<std::shared_ptr<Metrics::Reporter>> TestReporters{TestReporter};
  std::unique_ptr<Metrics::IRegistrar> TestRegistrar =
      std::make_unique<Metrics::Registrar>("Test", TestReporters);
};

TEST_F(FileWriterTask, WithPrefixFullFileNameIsCorrect) {
  FileWriter::FileWriterTask Task(TestRegistrar.get(),
                                  std::make_shared<MetaData::Tracker>());

  Task.setFullFilePath("SomePrefix", "File.hdf");

  ASSERT_EQ("SomePrefix/File.hdf", Task.filename());
}

TEST_F(FileWriterTask, WithoutPrefixFileNameIsCorrect) {
  FileWriter::FileWriterTask Task(TestRegistrar.get(),
                                  std::make_shared<MetaData::Tracker>());

  Task.setFullFilePath("/", "File.hdf");

  ASSERT_EQ("/File.hdf", Task.filename());
}

TEST_F(FileWriterTask, AddingSourceAddsToTopics) {
  FileWriter::FileWriterTask Task(TestRegistrar.get(),
                                  std::make_shared<MetaData::Tracker>());
  FileWriter::Source Src("Src1", "Id1", "Id2", "Topic1", nullptr);

  Task.addSource(std::move(Src));

  ASSERT_EQ(1u, Task.sources().size());
}

TEST_F(FileWriterTask, SettingJobIdSetsID) {
  FileWriter::FileWriterTask Task(TestRegistrar.get(),
                                  std::make_shared<MetaData::Tracker>());
  std::string NewId = "NewID";

  Task.setJobId(NewId);

  ASSERT_EQ(NewId, Task.jobID());
}
