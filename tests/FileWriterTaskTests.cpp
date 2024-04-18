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
  std::unique_ptr<Metrics::Sink> TestSink{
      std::make_unique<Metrics::MockSink>()};
  std::shared_ptr<Metrics::Reporter> TestReporter{
      std::make_shared<Metrics::Reporter>(std::move(TestSink), 10ms)};
  std::vector<std::shared_ptr<Metrics::Reporter>> TestReporters{TestReporter};
  std::unique_ptr<Metrics::IRegistrar> TestRegistrar =
      std::make_unique<Metrics::Registrar>("Test", TestReporters);
};

TEST_F(FileWriterTask, file_path_is_set) {
  FileWriter::FileWriterTask Task("::some_id::", {"/example/filepath.hdf"},
                                  TestRegistrar.get(),
                                  std::make_shared<MetaData::Tracker>());

  ASSERT_EQ("/example/filepath.hdf", Task.filename());
}

TEST_F(FileWriterTask, adding_source_adds_to_topics) {
  FileWriter::FileWriterTask Task("::some_id::", {"some_file_path.hdf"},
                                  TestRegistrar.get(),
                                  std::make_shared<MetaData::Tracker>());
  FileWriter::Source Src("Src1", "Id1", "Id2", "Topic1", nullptr);

  Task.addSource(std::move(Src));

  ASSERT_EQ(1u, Task.sources().size());
}

TEST_F(FileWriterTask, job_id_is_set) {
  FileWriter::FileWriterTask Task("NewID", {"some_file_path.hdf"},
                                  TestRegistrar.get(),
                                  std::make_shared<MetaData::Tracker>());

  ASSERT_EQ("NewID", Task.jobID());
}
