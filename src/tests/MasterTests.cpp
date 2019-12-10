// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "JobCreator.h"
#include "Master.h"
#include "helpers/FakeStreamMaster.h"
#include <memory>
#include <gtest/gtest.h>

class FakeJobCreator: public FileWriter::IJobCreator {
public:
  std::unique_ptr<FileWriter::IStreamMaster> createFileWritingJob(
      FileWriter::StartCommandInfo const &/*StartInfo*/,
      std::shared_ptr<KafkaW::ProducerTopic> const &/*StatusProducer*/,
      MainOpt &/*Settings*/,
      SharedLogger const &/*Logger*/) override {
    return std::make_unique<FakeStreamMaster>("some_id");
  };

};

std::string StartCommand{R"""({
    "cmd": "filewriter_new",
    "broker": "localhost:9092",
    "job_id": "1234",
    "file_attributes": {"file_name": "output_file1.nxs"},
    "nexus_structure": { }
  })"""};

std::string StopCommand{R"""({
    "cmd": "filewriter_stop",
    "job_id": "1234"
  })"""};

TEST(MasterTests, IfWritingStartedThenIsWriting) {
  MainOpt MainOpts;
  std::unique_ptr<FileWriter::IJobCreator> Creator = std::make_unique<FakeJobCreator>();
  FileWriter::Master Master(MainOpts, std::move(Creator));

  Master.handle_command(StartCommand, std::chrono::milliseconds{0});

  ASSERT_TRUE(Master.isWriting());
}

TEST(MasterTests, IfWritingStartedThenStoppedThenIsNotWriting) {
  MainOpt MainOpts;
  std::unique_ptr<FileWriter::IJobCreator> Creator = std::make_unique<FakeJobCreator>();
  FileWriter::Master Master(MainOpts, std::move(Creator));
  Master.handle_command(StartCommand, std::chrono::milliseconds{0});

  Master.handle_command(StopCommand, std::chrono::milliseconds{0});

  ASSERT_FALSE(Master.isWriting());
}

