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

namespace FileWriter {

namespace {

using nlohmann::json;

json hdf_parse(std::string const &Structure, SharedLogger const &Logger) {
  try {
    auto StructureDocument = json::parse(Structure);
    return StructureDocument;
  } catch (...) {
    Logger->error("Parse Error: ", Structure);
    throw FileWriter::ParseError(Structure);
  }
}
} // namespace

std::vector<Source> &FileWriterTask::sources() { return SourceToModuleMap; }

void FileWriterTask::setFilename(std::string const &Prefix,
                                 std::string const &Name) {
  if (Prefix.empty()) {
    Filename = Name;
  } else {
    Filename = Prefix + "/" + Name;
  }
}

void FileWriterTask::addSource(Source &&Source) {
  SourceToModuleMap.push_back(std::move(Source));
}

void FileWriterTask::InitialiseHdf(std::string const &NexusStructure,
                                   std::vector<StreamHDFInfo> &HdfInfo) {
  auto NexusStructureJson = hdf_parse(NexusStructure, Logger);
  std::string ErrorString;
  std::filesystem::path FilePath(Filename);

  if (std::filesystem::exists(Filename)) {
    ErrorString = fmt::format(
        "Failed to initialize HDF file \"{}\". Error was: \"{}\".", Filename,
        "a file with that filename already exists in that directory. Delete "
        "the existing file or provide another filename");
    std::throw_with_nested(std::runtime_error(ErrorString));
  } else if (not FilePath.has_filename()) {
    ErrorString =
        fmt::format("Failed to initialize HDF file \"{}\". Error was: \"{}\".",
                    Filename, "filename is empty");
    std::throw_with_nested(std::runtime_error(ErrorString));
  } else if (FilePath.has_parent_path() and
             not std::filesystem::exists(FilePath.parent_path())) {
    ErrorString = fmt::format("Failed to initialize HDF file \"{}\". Error "
                              "was: The parent directory does not exist.",
                              Filename);
    std::throw_with_nested(std::runtime_error(ErrorString));
  }

  try {
    Logger->info("Creating HDF file {}", Filename);
    File = std::make_unique<HDFFile>(Filename, NexusStructureJson, HdfInfo,
                                     MetaDataTracker);
  } catch (std::exception const &E) {
    ErrorString =
        fmt::format("Failed to initialize HDF file \"{}\". Error was: {}",
                    Filename, E.what());
    LOG_ERROR(ErrorString);
    std::throw_with_nested(std::runtime_error(ErrorString));
  }
}

std::string FileWriterTask::jobID() const { return JobId; }

hdf5::node::Group FileWriterTask::hdfGroup() const { return File->hdfGroup(); }

void FileWriterTask::switchToWriteMode() {
  if (File->isRegularMode()) {
    File->openInSWMRMode();
  }
}

bool FileWriterTask::isInWriteMode() { return File->isSWMRMode(); }

void FileWriterTask::setJobId(std::string const &Id) { JobId = Id; }

std::string FileWriterTask::filename() const { return Filename; }

void FileWriterTask::flushDataToFile() {
  if (File != nullptr) {
    File->flush();
  }
}

} // namespace FileWriter
