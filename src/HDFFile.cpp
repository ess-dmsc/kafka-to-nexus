// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDFFile.h"
#include "Filesystem.h"
#include "HDFAttributes.h"
#include "HDFOperations.h"
#include "HDFVersionCheck.h"
#include "Version.h"
#include "json.h"

namespace FileWriter {
using HDFOperations::createHDFStructures;
using HDFOperations::writeAttributesIfPresent;
using HDFOperations::writeHDFISO8601AttributeCurrentTime;

HDFFile::HDFFile(std::string const &FileName,
                 nlohmann::json const &NexusStructure,
                 std::vector<StreamHDFInfo> &StreamHDFInfo)
    : H5FileName(FileName) {
  if (FileName.empty()) {
    throw std::runtime_error("HDF file name must not be empty.");
  }
  createFileInRegularMode();
  init(NexusStructure, StreamHDFInfo);
  StoredNexusStructure = NexusStructure;
  closeFile();
  openFileInSWMRMode();
}

HDFFile::~HDFFile() {
  try {
    closeFile();
    openFileInRegularMode();
    addLinks();
  } catch (std::exception const &E) {
    LOG_ERROR("Unable to finish file \"{}\". Error message was: {}", H5FileName,
              E.what());
  }
}

void HDFFile::createFileInRegularMode() {
  hdfFile() = hdf5::file::create(H5FileName,
                                 hdf5::file::AccessFlags::EXCLUSIVE |
                                     hdf5::file::AccessFlags::SWMR_WRITE,
                                 {}, {});
}

void HDFFileBase::init(const std::string &NexusStructure,
                       std::vector<StreamHDFInfo> &StreamHDFInfo) {
  auto Document = nlohmann::json::parse(NexusStructure);
  init(Document, StreamHDFInfo);
}

void HDFFileBase::init(const nlohmann::json &NexusStructure,
                       std::vector<StreamHDFInfo> &StreamHDFInfo) {

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
      auto value = &NexusStructure;
      if (auto ChildrenMaybe = find<nlohmann::json>("children", *value)) {
        auto Children = *ChildrenMaybe;
        if (Children.is_array()) {
          for (auto &Child : Children) {
            createHDFStructures(&Child, RootGroup, 0, lcpl, var_string,
                                StreamHDFInfo, path, Logger);
          }
        }
      }
    }

    HDFAttributes::writeAttribute(RootGroup, "HDF5_Version",
                                  h5VersionStringLinked());
    HDFAttributes::writeAttribute(RootGroup, "file_name",
                                  hdfFile().id().file_name().string());
    HDFAttributes::writeAttribute(
        RootGroup, "creator",
        fmt::format("kafka-to-nexus commit {:.7}", GetVersion()));
    writeHDFISO8601AttributeCurrentTime(RootGroup, "file_time");
    writeAttributesIfPresent(RootGroup, NexusStructure);
  } catch (std::exception const &E) {
    Logger->critical("Failed to initialize  file={}  trace:\n{}",
                     hdfFile().id().file_name().string(),
                     hdf5::error::print_nested(E));
    std::throw_with_nested(std::runtime_error("HDFFile failed to initialize!"));
  }
}

void HDFFile::closeFile() {
  try {
    if (hdfFile().is_valid()) {
      Logger->trace("Closing file.");
      hdfFile().close();
      hdfFile() = hdf5::file::File();
    } else {
      Logger->error("File is not valid, unable to close.");
    }
  } catch (const std::runtime_error &E) {
    auto Trace = hdf5::error::print_nested(E);
    Logger->error("Got error when closing file \"{}\". Failure was: {}",
                  hdfFile().id().file_name().string(), Trace);
    std::throw_with_nested(std::runtime_error(fmt::format(
        "HDFFile failed to close.  Current Path: {}  Filename: {}  Trace:\n{}",
        fs::current_path().string(), hdfFile().id().file_name().string(),
        Trace)));
  }
}

void HDFFile::openFileInSWMRMode() {
  hdfFile() = hdf5::file::open(H5FileName,
                               hdf5::file::AccessFlags::READWRITE |
                                   hdf5::file::AccessFlags::SWMR_WRITE,
                               {});
}

void HDFFileBase::flush() {
  try {
    if (H5File.is_valid()) {
      H5File.flush(hdf5::file::Scope::GLOBAL);
    } else {
      LOG_ERROR("Unable to flush file due to it being invalid.");
    }
  } catch (const std::runtime_error &E) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("HDFFile failed to flush  what: {}", E.what())));
  }
}

void HDFFile::openFileInRegularMode() {
  hdfFile() =
      hdf5::file::open(H5FileName, hdf5::file::AccessFlags::READWRITE, {});
}

void HDFFile::addLinks() {
  HDFOperations::addLinks(hdfGroup(), StoredNexusStructure, Logger);
}

} // namespace FileWriter
