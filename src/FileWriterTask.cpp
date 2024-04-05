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

namespace {

using nlohmann::json;

json hdf_parse(std::string const &Structure) {
  try {
    auto StructureDocument = json::parse(Structure);
    return StructureDocument;
  } catch (...) {
    LOG_CRITICAL("JSON parse error: ", Structure);
    throw FileWriter::ParseError(Structure);
  }
}
} // namespace

std::vector<Source> &FileWriterTask::sources() { return SourceToModuleMap; }

void FileWriterTask::setFullFilePath(std::filesystem::path const &filepath) {
  filepath_ = filepath;
}

void FileWriterTask::addSource(Source &&Source) {
  SourceToModuleMap.push_back(std::move(Source));
}

void FileWriterTask::InitialiseHdf(std::string const &NexusStructure,
                                   std::vector<ModuleHDFInfo> &HdfInfo) {
  auto NexusStructureJson = hdf_parse(NexusStructure);
  std::string ErrorString;

  if (std::filesystem::exists(filepath_)) {
    ErrorString = fmt::format(
        R"(Failed to initialize HDF file "{}". Error was: "{}".)",
        filepath_.string(),
        "a file with that filename already exists in that directory. Delete "
        "the existing file or provide another filename");
    std::throw_with_nested(std::runtime_error(ErrorString));
  } else if (not filepath_.has_filename()) {
    ErrorString =
        fmt::format(R"(Failed to initialize HDF file "{}". Error was: "{}".)",
                    filepath_.string(), "filename is empty");
    std::throw_with_nested(std::runtime_error(ErrorString));
  } else if (filepath_.has_parent_path() and
             not std::filesystem::exists(filepath_.parent_path())) {
    ErrorString = fmt::format(
        R"(Failed to initialize HDF file "{}". Error was: The parent directory does not exist.)",
        filepath_.string());
    std::throw_with_nested(std::runtime_error(ErrorString));
  }

  try {
    LOG_INFO("Creating HDF file {}", filepath_.string());
    File = std::make_unique<HDFFile>(filepath_, NexusStructureJson, HdfInfo,
                                     MetaDataTracker);
  } catch (std::exception const &E) {
    ErrorString =
        fmt::format(R"(Failed to initialize HDF file "{}". Error was: {})",
                    filepath_.string(), E.what());
    LOG_CRITICAL(ErrorString);
    std::throw_with_nested(std::runtime_error(ErrorString));
  }
}

std::string FileWriterTask::jobID() const { return job_id_; }

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

std::string FileWriterTask::filename() const { return filepath_.string(); }

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
  auto size = std::filesystem::file_size(filepath_, ErrorCode);
  if (ErrorCode) {
    LOG_ERROR(
        R"(Unable to determine file size of the file "{}". The error was: {})",
        filepath_.string(), ErrorCode.message());
    return;
  }
  auto SizeValue = int(std::ceil(size / (1024 * 1024)));
  FileSizeMB.setValue(SizeValue);
  FileSizeMBMetric = SizeValue;
}

} // namespace FileWriter
