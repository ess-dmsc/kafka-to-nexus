// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FileWriterTask.h"
#include "HDFFile.h"
#include "Source.h"
#include "helper.h"
#include "logger.h"
#include <atomic>
#include <filesystem>

namespace FileWriter {

std::vector<Source> &FileWriterTask::sources() { return SourceToModuleMap; }

void FileWriterTask::setFullFilePath(std::filesystem::path const &filepath) {
  _filepath = filepath;
}

void FileWriterTask::addSource(Source &&Source) {
  SourceToModuleMap.push_back(std::move(Source));
}

void FileWriterTask::InitialiseHdf(nlohmann::json const &NexusStructure,
                                   std::vector<ModuleHDFInfo> &HdfInfo,
                                   std::filesystem::path const &TemplatePath,
                                   std::string const &InstrumentName) {
  std::string ErrorString;

  if (std::filesystem::exists(_filepath)) {
    ErrorString = fmt::format(
        R"(Failed to initialize HDF file "{}". Error was: "{}".)",
        _filepath.string(),
        "a file with that filename already exists in that directory. Delete "
        "the existing file or provide another filename");
    std::throw_with_nested(std::runtime_error(ErrorString));
  } else if (not _filepath.has_filename()) {
    ErrorString =
        fmt::format(R"(Failed to initialize HDF file "{}". Error was: "{}".)",
                    _filepath.string(), "filename is empty");
    std::throw_with_nested(std::runtime_error(ErrorString));
  } else if (_filepath.has_parent_path() and
             not std::filesystem::exists(_filepath.parent_path())) {
    ErrorString = fmt::format(
        R"(Failed to initialize HDF file "{}". Error was: The parent directory does not exist.)",
        _filepath.string());
    std::throw_with_nested(std::runtime_error(ErrorString));
  }

  try {
    Logger::Info("Creating HDF file {}", _filepath.string());
    File = std::make_unique<HDFFile>(_filepath, NexusStructure, HdfInfo,
                                     MetaDataTracker, TemplatePath, InstrumentName);
  } catch (std::exception const &E) {
    ErrorString =
        fmt::format(R"(Failed to initialize HDF file "{}". Error was: {})",
                    _filepath.string(), E.what());
    Logger::Critical(ErrorString);
    std::throw_with_nested(std::runtime_error(ErrorString));
  }
}

std::string FileWriterTask::jobID() const { return _job_id; }

hdf5::node::Group FileWriterTask::hdfGroup() const {
  if (!File) {
    throw std::runtime_error(
        "Could not obtain group as no HDF file currently open");
  }

  return File->hdfGroup();
}

void FileWriterTask::switchToWriteMode() {
  if (File->isRegularMode()) {
    File->openInSWMRMode();
  }
}

bool FileWriterTask::isInWriteMode() { return File->isSWMRMode(); }

std::string FileWriterTask::filename() const { return _filepath.string(); }

void FileWriterTask::writeLinks(
    const std::vector<ModuleSettings> &LinkSettingsList) {
  File->addLinks(LinkSettingsList);
}

void FileWriterTask::writeMetaData() { File->addMetaData(); }

void FileWriterTask::flushDataToFile() {
  if (File != nullptr) {
    File->flush();
  }
}

void FileWriterTask::updateApproximateFileSize() {
  std::error_code ErrorCode;
  auto size = std::filesystem::file_size(_filepath, ErrorCode);
  if (ErrorCode) {
    Logger::Error(
        R"(Unable to determine file size of the file "{}". The error was: {})",
        _filepath.string(), ErrorCode.message());
    return;
  }
  auto SizeValue = int(std::ceil(size / (1024 * 1024)));
  FileSizeMB.setValue(SizeValue);
  FileSizeMBMetric = SizeValue;
}

} // namespace FileWriter
