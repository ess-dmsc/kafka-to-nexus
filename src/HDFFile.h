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

  static std::string h5VersionStringLinked();
  static void writeAttributes(hdf5::node::Node const &Node,
                              nlohmann::json const *Value);

  static void writeStringAttribute(hdf5::node::Node const &Node,
                                   std::string const &Name,
                                   std::string const &Value);

  /// If using SWMR, gets invoked by Source and can trigger a flush of the HDF
  /// file.
  void SWMRFlush();

  bool isSWMREnabled() const;

  hdf5::file::File H5File;
  hdf5::node::Group RootGroup;

private:
  friend class ::T_HDFFile;
  friend class CommandHandler;

  static void checkHDFVersion();
  static std::string H5VersionStringHeadersCompileTime();

  static void createHDFStructures(
      const nlohmann::json *Value, hdf5::node::Group const &Parent,
      uint16_t Level,
      hdf5::property::LinkCreationList const &LinkCreationPropertyList,
      hdf5::datatype::String const &FixedStringHDFType,
      std::vector<StreamHDFInfo> &HDFStreamInfo, std::deque<std::string> &Path);

  static void writeHDFISO8601AttributeCurrentTime(hdf5::node::Node const &Node,
                                                  const std::string &Name);

  static void writeAttributesIfPresent(hdf5::node::Node const &Node,
                                       nlohmann::json const &Values);

  static std::vector<std::string> populateStrings(const nlohmann::json *Values,
                                                  hssize_t GoalSize);

  static void
  writeStringDataset(hdf5::node::Group const &Parent, const std::string &Name,
                     hdf5::property::DatasetCreationList &DatasetCreationList,
                     hdf5::dataspace::Dataspace &Dataspace,
                     nlohmann::json const &Values);

  static void writeFixedSizeStringDataset(
      hdf5::node::Group const &Parent, const std::string &Name,
      hdf5::property::DatasetCreationList &DatasetCreationList,
      hdf5::dataspace::Dataspace &Dataspace, hsize_t ElementSize,
      const nlohmann::json *Values);

  static void writeGenericDataset(const std::string &DataType,
                                  hdf5::node::Group const &Parent,
                                  const std::string &Name,
                                  const std::vector<hsize_t> &Sizes,
                                  const std::vector<hsize_t> &Max,
                                  hsize_t ElementSize,
                                  const nlohmann::json *Values);

  static void writeDataset(hdf5::node::Group const &Parent,
                           const nlohmann::json *Values);

  static void
  setCommonProps(hdf5::property::FileCreationList &FileCreationPropertyList,
                 hdf5::property::FileAccessList &FileAccessPropertyList) {}

  static void writeObjectOfAttributes(hdf5::node::Node const &Node,
                                      const nlohmann::json &Values);

  static void writeArrayOfAttributes(hdf5::node::Node const &Node,
                                     const nlohmann::json &Values);

  static void writeScalarAttribute(hdf5::node::Node const &Node,
                                   const std::string &Name,
                                   const nlohmann::json &Values);

  static void writeAttrOfSpecifiedType(
      std::string const &DType, hdf5::node::Node const &Node,
      std::string const &Name, uint32_t StringSize,
      hdf5::datatype::CharacterEncoding Encoding, nlohmann::json const &Values);

  bool SWMREnabled = false;
  std::string Filename;
  nlohmann::json NexusStructure;

  using CLOCK = std::chrono::steady_clock;
  std::chrono::milliseconds SWMRFlushInterval{10000};
  std::chrono::time_point<CLOCK> SWMRFlushLast = CLOCK::now();
};

} // namespace FileWriter
