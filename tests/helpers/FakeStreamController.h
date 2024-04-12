// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "StreamController.h"
#include <chrono>
#include <memory>
#include <string>

class FakeStreamController : public FileWriter::IStreamController {
public:
  explicit FakeStreamController(std::string const &JobID,
                                bool Removable = false)
      : JobID(JobID), IsRemovable(Removable) {}
  std::string getJobId() const override { return JobID; }
  void setStopTime(const std::chrono::milliseconds & /*StopTime*/) override {
    // Simulate immediate stop.
    IsRemovable = true;
  }
  bool isDoneWriting() override { return IsRemovable; }

private:
  std::string JobID;
  bool IsRemovable;
};
