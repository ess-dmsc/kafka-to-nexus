#pragma once

#include "Msg.h"
#include "json.h"
#include <H5Ipublic.h>
#include <chrono>
#include <h5cpp/hdf5.hpp>
#include <memory>
#include <string>
#include <vector>

class T_HDFFile;

namespace FileWriter {

// POD
struct StreamHDFInfo {
  std::string HDFParentName;
  std::string ConfigStream;
  bool InitialisedOk = false;
};

class HDFFile final {
public:
  HDFFile();

  ~HDFFile();

  void init(const std::string &Filename, nlohmann::json const &NexusStructure,
            nlohmann::json const &ConfigFile,
            std::vector<StreamHDFInfo> &StreamHDFInfo, bool UseHDFSWMR);

  void init(const std::string &NexusStructure,
            std::vector<StreamHDFInfo> &StreamHDFInfo);

  void init(const nlohmann::json &NexusStructure,
            std::vector<StreamHDFInfo> &StreamHDFInfo);

  void reopen(std::string const &Filename);

  void flush();
  void close();
  void finalize();

  /// If using SWMR, gets invoked by Source and can trigger a flush of the HDF
  /// file.
  void SWMRFlush();

  bool isSWMREnabled() const;

  hdf5::file::File H5File;
  hdf5::node::Group RootGroup;

private:
  friend class ::T_HDFFile;
  friend class CommandHandler;

  void
  setCommonProps(hdf5::property::FileCreationList &FileCreationPropertyList,
                 hdf5::property::FileAccessList &FileAccessPropertyList) {}

  bool SWMREnabled = false;
  std::string Filename;
  nlohmann::json NexusStructure;

  using CLOCK = std::chrono::steady_clock;
  std::chrono::milliseconds SWMRFlushInterval{10000};
  std::chrono::time_point<CLOCK> SWMRFlushLast = CLOCK::now();
  std::shared_ptr<spdlog::logger> Logger = spdlog::get("filewriterlogger");
};

std::string h5VersionStringLinked();
void writeAttributes(hdf5::node::Node const &Node, nlohmann::json const *Value,
                     std::shared_ptr<spdlog::logger> Logger);

void writeStringAttribute(hdf5::node::Node const &Node, std::string const &Name,
                          std::string const &Value);

void checkHDFVersion(std::shared_ptr<spdlog::logger> Logger);
std::string H5VersionStringHeadersCompileTime();

void createHDFStructures(
    const nlohmann::json *Value, hdf5::node::Group const &Parent,
    uint16_t Level,
    hdf5::property::LinkCreationList const &LinkCreationPropertyList,
    hdf5::datatype::String const &FixedStringHDFType,
    std::vector<StreamHDFInfo> &HDFStreamInfo, std::deque<std::string> &Path,
    std::shared_ptr<spdlog::logger> Logger);

void writeHDFISO8601AttributeCurrentTime(
    hdf5::node::Node const &Node, const std::string &Name,
    std::shared_ptr<spdlog::logger> Logger);

void writeAttributesIfPresent(hdf5::node::Node const &Node,
                              nlohmann::json const &Values,
                              std::shared_ptr<spdlog::logger> Logger);

std::vector<std::string> populateStrings(const nlohmann::json *Values,
                                         hssize_t GoalSize);

void writeStringDataset(
    hdf5::node::Group const &Parent, const std::string &Name,
    hdf5::property::DatasetCreationList &DatasetCreationList,
    hdf5::dataspace::Dataspace &Dataspace, nlohmann::json const &Values);

void writeFixedSizeStringDataset(
    hdf5::node::Group const &Parent, const std::string &Name,
    hdf5::property::DatasetCreationList &DatasetCreationList,
    hdf5::dataspace::Dataspace &Dataspace, hsize_t ElementSize,
    const nlohmann::json *Values, std::shared_ptr<spdlog::logger> Logger);

void writeGenericDataset(const std::string &DataType,
                         hdf5::node::Group const &Parent,
                         const std::string &Name,
                         const std::vector<hsize_t> &Sizes,
                         const std::vector<hsize_t> &Max, hsize_t ElementSize,
                         const nlohmann::json *Values,
                         std::shared_ptr<spdlog::logger> Logger);

void writeDataset(hdf5::node::Group const &Parent, const nlohmann::json *Values,
                  std::shared_ptr<spdlog::logger> Logger);

void writeObjectOfAttributes(hdf5::node::Node const &Node,
                             const nlohmann::json &Values);

void writeArrayOfAttributes(hdf5::node::Node const &Node,
                            const nlohmann::json &Values,
                            std::shared_ptr<spdlog::logger> Logger);

void writeScalarAttribute(hdf5::node::Node const &Node, const std::string &Name,
                          const nlohmann::json &Values);

void writeAttrOfSpecifiedType(std::string const &DType,
                              hdf5::node::Node const &Node,
                              std::string const &Name, uint32_t StringSize,
                              hdf5::datatype::CharacterEncoding Encoding,
                              nlohmann::json const &Values,
                              std::shared_ptr<spdlog::logger> Logger);
} // namespace FileWriter
