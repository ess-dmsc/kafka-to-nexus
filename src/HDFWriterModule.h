#pragma once

#include "FlatbufferMessage.h"
#include <fmt/format.h>
#include <functional>
#include <h5cpp/hdf5.hpp>
#include <map>
#include <memory>
#include <string>

namespace FileWriter {

namespace HDFWriterModule_detail {

/// \brief Result type for the initialization of the writer module.
enum class InitResult { ERROR_IO = -1, OK = 0 };

/// Result type for write operation on the writer module.
enum class WriteResult { ERROR_BAD_FLATBUFFER = -2, ERROR_IO = -1, OK = 0 };

} // namespace HDFWriterModule_detail

/// \brief Writes a given flatbuffer to HDF.
///
/// Base class for the writer modules which are responsible for actually
/// writing a flatbuffer message to the HDF file.  A HDFWriterModule is
/// instantiated for each 'stream' which is configured in a file writer json
/// command.  The HDFWriterModule class registers itself via a string id which
/// must be unique.  This id is used in the file writer json command.  The id
/// can be arbitrary but should as a convention contain the flatbuffer schema
/// id (`FBID`) like `FBID_<writer-module-name>`.
/// Example: Please see `src/schemas/ev42/ev42_rw.cpp`.
class HDFWriterModule {
public:
  using ptr = std::unique_ptr<HDFWriterModule>;
  using InitResult = HDFWriterModule_detail::InitResult;
  using WriteResult = HDFWriterModule_detail::WriteResult;

  virtual ~HDFWriterModule() = default;

  /// \brief Parses the configuration of a stream.
  ///
  /// \param config_stream Configuration from the write file command for this
  /// stream.
  /// \param config_module Configuration for all instances of this
  /// HDFWriterModule.
  virtual void parse_config(std::string const &ConfigurationStream,
                            std::string const &ConfigurationModule) = 0;

  /// \brief Initialise the HDF file.
  ///
  /// Called before any data has arrived with the json configuration of this
  /// stream to allow the `HDFWriterModule` to create any structures in the HDF
  /// file.
  ///
  /// \param[in] HDFGroup     The \p HDFGroup into which this HDFWriterModule
  /// should write its data.
  /// \param[in] HDFAttributes Additional attributes as defined in the Nexus
  /// structure which the HDFWriterModule should write to the file. Because the
  /// HDFWriterModule is free to create the structure and data sets according to
  /// its needs, it must also take the responsibility to write these
  /// attributes.
  /// \param[in] HDFAttributes Json string of the attributes associated with the
  /// stream, as defined by the "attributes" key in the Nexus structure.
  ///
  /// \return The result.
  virtual InitResult init_hdf(hdf5::node::Group &HDFGroup,
                              std::string const &HDFAttributes) = 0;

  /// \brief Reopen the HDF objects which are used by this HDFWriterModule.
  ///
  /// \param InitParameters Contains most importantly the \p HDFGroup into
  /// which this HDFWriterModule should write its data.
  ///
  /// \return The result.
  virtual InitResult reopen(hdf5::node::Group &HDFGroup) = 0;

  /// \brief Process the message in some way, for example write to the HDF file.
  ///
  /// \param msg The message to process
  ///
  /// \return The result.
  virtual WriteResult write(FlatbufferMessage const &Message) = 0;

  /// \brief Flush the internal buffer.
  ///
  /// You are expected to flush all the internal buffers which you have to
  /// the HDF file.
  ///
  /// \return Error code.
  virtual int32_t flush() = 0;

  /// \brief Close all open HDF handlers.
  ///
  /// \return Error code.
  virtual int32_t close() = 0;
};

/// \brief Keeps track of the registered FlatbufferReader instances.
///
/// Writer modules register themselves via instantiation of the `Registrar`.
/// See for example `src/schemas/ev42/ev42_rw.cxx` and search for
/// HDFWriterModuleRegistry.
namespace HDFWriterModuleRegistry {
using ModuleFactory = std::function<std::unique_ptr<HDFWriterModule>()>;

/// \brief Get all registered modules.
///
/// \return A reference to the map of registered modules.
std::map<std::string, ModuleFactory> &getFactories();

/// \brief Registers a new writer module. Called by `Registrar`.
///
/// \param key
/// \param value
void addWriterModule(std::string const &Key, ModuleFactory Value);

/// \brief Get `ModuleFactory for a given `key`.
///
/// \return Matching `ModuleFactory`.
ModuleFactory &find(std::string const &key);

/// \brief  Registers the writer module at program start if instantiated in the
/// namespace of each writer module with the writer module given as `Module`.
template <typename Module> class Registrar {
public:
  /// \brief Register the writer module given in template parameter `Module`
  /// under the
  /// identifier `FlatbufferID`.
  ///
  /// \param FlatbufferID The unique identifier for this writer module.
  explicit Registrar(std::string const &FlatbufferID) {
    auto FactoryFunction = []() { return std::make_unique<Module>(); };
    addWriterModule(FlatbufferID, FactoryFunction);
  };
};

class WriterException : public std::runtime_error {
public:
  WriterException(const char *ErrorMessage)
      : std::runtime_error(ErrorMessage) {}
};
} // namespace HDFWriterModuleRegistry
} // namespace FileWriter
