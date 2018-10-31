#include <gtest/gtest.h>
#include "FileWriterTask.h"
#include "Source.h"

TEST(FileWriterTask, FullFileNameIsCorrect) {
  FileWriter::FileWriterTask Task("SomeID", nullptr);

  Task.setFilename("SomePrefix", "File.hdf");

  ASSERT_EQ("SomePrefix/File.hdf", Task.filename());
}

TEST(FileWriterTask, AddingSourceAddsToDemuxers) {
  FileWriter::FileWriterTask Task("SomeID", nullptr);
  FileWriter::Source Src("Src1", "Id1", nullptr);

  Task.addSource(std::move(Src));

  ASSERT_EQ(1, Task.demuxers().size());
}

TEST(FileWriterTask, SettingJobIdSetsID) {
  FileWriter::FileWriterTask Task("SomeID", nullptr);
  std::string NewId = "NewID";

  Task.setJobId(NewId);

  ASSERT_EQ(NewId, Task.jobID());
}
