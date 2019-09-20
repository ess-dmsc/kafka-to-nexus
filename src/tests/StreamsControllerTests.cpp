// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "StreamsController.h"
#include "StreamMaster.h"
#include <gtest/gtest.h>
#include <memory>

class FakeStreamMaster : public FileWriter::IStreamMaster {
public:
  explicit FakeStreamMaster(std::string const & JobID, bool Removable=false): JobID(JobID), IsRemovable(Removable) {}
  std::string getJobId() const override {return JobID;}
  void requestStop() override {
    IsRemovable = true;
  }
  bool isRemovable() const override {
    return IsRemovable;
  }
  void setStopTime(const std::chrono::milliseconds &/*StopTime*/) override {

  }
private:
  std::string JobID;
  bool IsRemovable;

};

TEST(StreamsControllerTests, AddedStreamMasterCanBeRetrieved) {
  std::string JobID = "job_id";
  FileWriter::StreamsController Controller;
  auto StreamMaster = std::make_unique<FakeStreamMaster>(JobID);

  Controller.addStreamMaster(std::move(StreamMaster));

  ASSERT_EQ(JobID, Controller.getStreamMasterForJobID(JobID)->getJobId());
}

TEST(StreamsControllerTests, RequestingNonExistingJobIDThrows) {
  FileWriter::StreamsController Controller;

  ASSERT_THROW(Controller.getStreamMasterForJobID("NOT-USED"), std::runtime_error);
}

TEST(StreamsControllerTests, StreamMastersCanBeStopped) {
  std::string JobID1 = "job_id_1";
  std::string JobID2 = "job_id_2";
  FileWriter::StreamsController Controller;
  Controller.addStreamMaster(std::make_unique<FakeStreamMaster>(JobID1));
  Controller.addStreamMaster(std::make_unique<FakeStreamMaster>(JobID2));

  Controller.stopStreamMasters();

  ASSERT_TRUE(Controller.getStreamMasterForJobID(JobID1)->isRemovable());
  ASSERT_TRUE(Controller.getStreamMasterForJobID(JobID2)->isRemovable());
}

TEST(StreamsControllerTests, RemovableStreamMastersAreDeleted) {
  std::string JobID1 = "job_id_1";
  std::string JobID2 = "job_id_2";
  FileWriter::StreamsController Controller;
  Controller.addStreamMaster(std::make_unique<FakeStreamMaster>(JobID1));
  Controller.addStreamMaster(std::make_unique<FakeStreamMaster>(JobID2));
  Controller.stopStreamMasters();

  Controller.deleteRemovable();

  ASSERT_THROW(Controller.getStreamMasterForJobID(JobID1), std::runtime_error);
  ASSERT_THROW(Controller.getStreamMasterForJobID(JobID2), std::runtime_error);
}

TEST(StreamsControllerTests, RemovableStreamMasterIsDeleted) {
  std::string JobID1 = "job_id_1";
  std::string JobID2 = "job_id_2";
  FileWriter::StreamsController Controller;
  Controller.addStreamMaster(std::make_unique<FakeStreamMaster>(JobID1));
  Controller.addStreamMaster(std::make_unique<FakeStreamMaster>(JobID2, true));

  Controller.deleteRemovable();

  // Not Deleted
  ASSERT_EQ(JobID1, Controller.getStreamMasterForJobID(JobID1)->getJobId());
  // Deleted
  ASSERT_THROW(Controller.getStreamMasterForJobID(JobID2), std::runtime_error);
}

