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
#include "ModuleHDFInfo.h"
#include "ModuleSettings.h"
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
            std::vector<ModuleHDFInfo> &ModuleHDFInfo,
            std::filesystem::path const &template_path,
            bool const &is_legacy_writing);

  void init(const nlohmann::json &NexusStructure,
            std::vector<ModuleHDFInfo> &ModuleHDFInfo,
            std::filesystem::path const &template_path,
            bool const &is_legacy_writing);

private:
  void write_nexus_file_metadata(hdf5::node::Group const &RootGroup,
                                 nlohmann::json const &NexusStructure);
  void write_template_file_metadata(hdf5::node::Group const &RootGroup,
                                    nlohmann::json const &NexusStructure);
  void write_common_attributes(hdf5::node::Group const &RootGroup);
  void write_dynamic_version_if_present(hdf5::node::Group const &RootGroup,
                                        nlohmann::json const &NexusStructure);
  void write_template_version_if_present(hdf5::node::Group const &RootGroup,
                                         nlohmann::json const &NexusStructure);
  std::string
  read_template_version_if_present(hdf5::node::Group const &RootGroup);
  std::string ExistingTemplateVersion;
  hdf5::file::File H5File;
};

class HDFFile : public HDFFileBase {
public:
  HDFFile(std::filesystem::path const &FileName,
          nlohmann::json const &NexusStructure,
          std::vector<ModuleHDFInfo> &ModuleHDFInfo,
          MetaData::TrackerPtr &TrackerPtr,
          std::filesystem::path const &template_path,
          bool const &is_legacy_writing);
  void addLinks(std::vector<ModuleSettings> const &LinkSettingsList);
  void addMetaData();
  void openInSWMRMode();
  void openInRegularMode();
  bool isSWMRMode() const;
  bool isRegularMode() const;
  virtual ~HDFFile();

private:
  bool SWMRMode{false};
  void createFileInRegularMode(std::filesystem::path const &template_path,
                               bool const &is_legacy_writing);
  void openFileInRegularMode();
  void openFileInSWMRMode();
  void safeClose();
  void closeFile();

  hdf5::property::FileAccessList FileAccessList;
  hdf5::property::FileCreationList FileCreationList;

  std::filesystem::path H5FileName;
  nlohmann::json StoredNexusStructure;
  MetaData::TrackerPtr const MetaDataTracker;
};

} // namespace FileWriter
