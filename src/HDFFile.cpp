// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDFFile.h"
#include "HDFAttributes.h"
#include "HDFOperations.h"
#include "HDFVersionCheck.h"
#include "Version.h"
#include "json.h"

namespace FileWriter {
using HDFOperations::createHDFStructures;
using HDFOperations::writeAttributesIfPresent;
using HDFOperations::writeHDFISO8601AttributeCurrentTime;

HDFFile::HDFFile(std::filesystem::path const &FileName,
                 nlohmann::json const &NexusStructure,
                 std::vector<ModuleHDFInfo> &ModuleHDFInfo,
                 MetaData::TrackerPtr &TrackerPtr,
                 std::filesystem::path const &template_path,
                 bool const &is_legacy_writing)
    : H5FileName(FileName), MetaDataTracker(TrackerPtr) {
  if (FileName.empty()) {
    throw std::runtime_error("HDF file name must not be empty.");
  }
  FileAccessList.library_version_bounds(hdf5::property::LibVersion::Latest,
                                        hdf5::property::LibVersion::Latest);
  createFileInRegularMode(template_path, is_legacy_writing);
  init(NexusStructure, ModuleHDFInfo, template_path, is_legacy_writing);
  StoredNexusStructure = NexusStructure;
}

HDFFile::~HDFFile() {
  addMetaData();
  std::filesystem::permissions(H5FileName,
                               std::filesystem::perms::owner_read |
                                   std::filesystem::perms::group_read |
                                   std::filesystem::perms::others_read,
                               std::filesystem::perm_options::replace);
  Logger::Debug("Closing file from destructor.");
  safeClose();
}

void HDFFile::createFileInRegularMode(
    std::filesystem::path const &template_path, bool const &is_legacy_writing) {
  if (hdfFile().is_valid()) { //	in case the last file handle was not
                              // destroyed properly
    // (if we don't add this 99% of files will complain that there was no valid
    // file to begin with)
    Logger::Debug("Closing file from createFileInRegularMode(...).");
    safeClose();
  }
  if (template_path.empty() || is_legacy_writing) {
    Logger::Info("Creating new file: {}", H5FileName.string());
    hdfFile() = hdf5::file::create(H5FileName,
                                   hdf5::file::AccessFlags::Exclusive |
                                       hdf5::file::AccessFlags::SWMRWrite,
                                   FileCreationList, FileAccessList);
  } else {
    Logger::Info("Copying template file: {} to: {}", template_path.string(),
                 H5FileName.string());
    std::filesystem::copy(template_path, H5FileName,
                          std::filesystem::copy_options::overwrite_existing);
    hdfFile() = hdf5::file::open(H5FileName,
                                 hdf5::file::AccessFlags::ReadWrite |
                                     hdf5::file::AccessFlags::SWMRWrite,
                                 FileAccessList);
  }
}

void HDFFileBase::init(const std::string &NexusStructure,
                       std::vector<ModuleHDFInfo> &ModuleHDFInfo,
                       std::filesystem::path const &template_path,
                       bool const &is_legacy_writing) {
  auto Document = nlohmann::json::parse(NexusStructure);
  init(Document, ModuleHDFInfo, template_path, is_legacy_writing);
}

void HDFFileBase::init(const nlohmann::json &NexusStructure,
                       std::vector<ModuleHDFInfo> &ModuleHDFInfo,
                       std::filesystem::path const &template_path,
                       bool const &is_legacy_writing) {

  try {
    hdf5::property::AttributeCreationList acpl;
    acpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);

    hdf5::property::LinkCreationList lcpl;
    lcpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);

    auto var_string = hdf5::datatype::String::variable();
    var_string.encoding(hdf5::datatype::CharacterEncoding::UTF8);

    auto RootGroup = hdfGroup();

    std::deque<std::string> path;
    if (NexusStructure.is_object()) {
      if (auto Children = find<nlohmann::json>("children", NexusStructure)) {
        if (Children->is_array()) {
          for (auto &Child : *Children) {
            createHDFStructures(Child, RootGroup, 0, lcpl, var_string,
                                ModuleHDFInfo, path);
          }
        }
      }
    }

    if (!template_path.empty() || is_legacy_writing) {
      write_nexus_file_metadata(RootGroup, NexusStructure);
    } else {
      write_template_version_if_present(RootGroup, NexusStructure);
    }

    writeAttributesIfPresent(RootGroup, NexusStructure);
  } catch (std::exception const &E) {
    Logger::Critical("Failed to initialize  file={}  trace:\n{}",
                     hdfFile().id().file_name().string(),
                     hdf5::error::print_nested(E));
    throw std::runtime_error("HDFFile failed to initialize!");
  }
}

void HDFFileBase::write_nexus_file_metadata(
    hdf5::node::Group const &RootGroup, nlohmann::json const &NexusStructure) {
  write_common_attributes(RootGroup);
  write_dynamic_version_if_present(RootGroup, NexusStructure);
  ExistingTemplateVersion = read_template_version_if_present(RootGroup);
}

void HDFFileBase::write_template_file_metadata(
    hdf5::node::Group const &RootGroup, nlohmann::json const &NexusStructure) {
  write_template_version_if_present(RootGroup, NexusStructure);
}

void HDFFileBase::write_common_attributes(hdf5::node::Group const &RootGroup) {
  HDFAttributes::writeAttribute(RootGroup, "HDF5_Version",
                                h5VersionStringLinked());
  HDFAttributes::writeAttribute(RootGroup, "file_name",
                                hdfFile().id().file_name().string());
  HDFAttributes::writeAttribute(
      RootGroup, "creator",
      fmt::format("kafka-to-nexus commit {}", GetVersion()));
  writeHDFISO8601AttributeCurrentTime(RootGroup, "file_time");
}

void HDFFileBase::write_dynamic_version_if_present(
    hdf5::node::Group const &RootGroup, nlohmann::json const &NexusStructure) {
  if (NexusStructure.contains("dynamic_version") &&
      NexusStructure["dynamic_version"].is_string()) {
    HDFAttributes::writeAttribute(
        RootGroup, "dynamic_version",
        NexusStructure["dynamic_version"].get<std::string>());
  }
}

void HDFFileBase::write_template_version_if_present(
    hdf5::node::Group const &RootGroup, nlohmann::json const &NexusStructure) {
  if (NexusStructure.contains("template_version") &&
      NexusStructure["template_version"].is_string()) {
    HDFAttributes::writeAttribute(
        RootGroup, "template_version",
        NexusStructure["template_version"].get<std::string>());
  }
}

std::string HDFFileBase::read_template_version_if_present(
    hdf5::node::Group const &RootGroup) {
  if (RootGroup.attributes.exists("template_version")) {
    RootGroup.attributes["template_version"].read(ExistingTemplateVersion);
    Logger::Debug("The template version is: {}", ExistingTemplateVersion);
    return ExistingTemplateVersion;
  }
  Logger::Debug("No template version found.");
  return "";
}

void HDFFile::closeFile() {
  Logger::Debug("Closing file from closeFile().");
  safeClose();
  hdfFile() = hdf5::file::File();
}

void HDFFile::safeClose() {
  try {
    if (hdfFile().is_valid()) {
			hdfFile.flush();
      Logger::Debug(R"(Closing file "{}".)",
                    hdfFile().id().file_name().string());
      hdfFile().close();
    } else {
      Logger::Debug("File is not valid, unable to close.");
    }
  } catch (const std::runtime_error &E) {
    auto Trace = hdf5::error::print_nested(E);
    Logger::Critical(R"(Got error when closing file "{}". Failure was: {})",
                     hdfFile().id().file_name().string(), Trace);
    throw std::runtime_error(fmt::format(
        "HDFFile failed to close.  Current Path: {}  Filename: {}  Trace:\n{}",
        fs::current_path().string(), hdfFile().id().file_name().string(),
        Trace));
  }
}

void HDFFile::openFileInSWMRMode() {
  Logger::Debug(R"(Opening file "{}" in SWMR mode.)", H5FileName.string());
  hdfFile() = hdf5::file::open(H5FileName,
                               hdf5::file::AccessFlags::ReadWrite |
                                   hdf5::file::AccessFlags::SWMRWrite,
                               FileAccessList);
}

void HDFFileBase::flush() {
  try {
    if (H5File.is_valid()) {
      H5File.flush(hdf5::file::Scope::Global);
    } else {
      Logger::Critical("Unable to flush file due to it being invalid.");
    }
  } catch (const std::runtime_error &E) {
    throw std::runtime_error(
        fmt::format("HDFFile failed to flush  what: {}", E.what()));
  }
}

void HDFFile::openFileInRegularMode() {
  Logger::Debug(R"(Opening file "{}" in regular (non SWMR) mode.)",
                H5FileName.string());
  hdfFile() = hdf5::file::open(H5FileName, hdf5::file::AccessFlags::ReadWrite,
                               FileAccessList);
}

void HDFFile::addLinks(std::vector<ModuleSettings> const &LinkSettingsList) {
  try {
    openInRegularMode();
    HDFOperations::addLinks(hdfGroup(), LinkSettingsList);
  } catch (std::exception const &E) {
    Logger::Error(
        R"(Unable to finish file "{}". addLinks failed with error: {})",
        H5FileName.string(), E.what());
  }
}

void HDFFile::addMetaData() {
  try {
    openInRegularMode();
    if (MetaDataTracker != nullptr) {
      auto root = hdfFile().root();
      MetaDataTracker->writeToHDF5File(root);
    }
  } catch (std::exception const &E) {
    Logger::Error(
        R"(Unable to finish file "{}". addMetadata failed with error: {})",
        H5FileName.string(), E.what());
  }
}

void HDFFile::openInSWMRMode() {
  if (!SWMRMode) {
    closeFile();
    openFileInSWMRMode();
    SWMRMode = true;
  }
}

void HDFFile::openInRegularMode() {
  if (SWMRMode) {
    closeFile();
    openFileInRegularMode();
    SWMRMode = false;
  }
}

bool HDFFile::isSWMRMode() const { return SWMRMode; }

bool HDFFile::isRegularMode() const { return !SWMRMode; }

} // namespace FileWriter
