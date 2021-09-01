// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "MetaData/Tracker.h"
#include "StreamHDFInfo.h"
#include "json.h"
#include "logger.h"
#include <H5Ipublic.h>
#include <chrono>
#include <deque>
#include <h5cpp/hdf5.hpp>
#include <memory>
#include <string>
#include <vector>

namespace FileWriter {

class HDFFileBase {
public:
  virtual ~HDFFileBase() = default;
  virtual void flush();

  auto hdfGroup() const { return H5File.root(); }

protected:
  auto &hdfFile() { return H5File; }
  void init(const std::string &NexusStructure,
            std::vector<StreamHDFInfo> &StreamHDFInfo);

  void init(const nlohmann::json &NexusStructure,
            std::vector<StreamHDFInfo> &StreamHDFInfo);

  SharedLogger Logger = getLogger();

private:
  hdf5::file::File H5File;
};

class HDFFile : public HDFFileBase {
public:
  HDFFile(std::string const &FileName, nlohmann::json const &NexusStructure,
          std::vector<StreamHDFInfo> &StreamHDFInfo,
          MetaData::TrackerPtr &TrackerPtr);
  void openInSWMRMode();
  void openInRegularMode();
  bool isSWMRMode();
  bool isRegularMode();
  virtual ~HDFFile();

private:
  bool SWMRMode{false};
  void createFileInRegularMode();
  void openFileInRegularMode();
  void openFileInSWMRMode();
  void closeFile();
  void addLinks();

  std::string H5FileName;
  nlohmann::json StoredNexusStructure;
  MetaData::TrackerPtr const MetaDataTracker;
};

} // namespace FileWriter
