#pragma once

#include "CollectiveQueue.h"
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
class InitResult {
public:
  /// Everything was fine.
  ///
  /// \return The okay result.
  static InitResult OK() { return InitResult(0); }

  /// I/O error, for example if libhdf returned with a I/O error.
  ///
  /// \return The error result.
  static inline InitResult ERROR_IO() { return InitResult(-1); }

  /// The writer module needs more configuration that was is available.
  ///
  /// \return The incomplete configuration error result.
  static inline InitResult ERROR_INCOMPLETE_CONFIGURATION() {
    return InitResult(-2);
  }

  /// Indicates if status is okay.
  ///
  /// \return True if okay.
  inline bool is_OK() const { return v == 0; }

  /// Indicates if any error has occurred. More specific query function will
  /// come as need arises.
  ///
  /// \return True if any error has occurred
  inline bool is_ERR() const { return v < 0; }

  /// Used for status reports.
  ///
  /// \return The status.
  std::string to_str() const;

private:
  explicit inline InitResult(int8_t v) : v(v) {}
  int8_t v = -1;
};

/// Result type for write operation on the writer module. Not an enum but a
/// class because we have a message for the sad code path.
class WriteResult {
public:
  /// Everything was fine.
  ///
  /// \return The okay result.
  static WriteResult OK() { return WriteResult(0); }
  static WriteResult OK_WITH_TIMESTAMP(uint64_t timestamp) {
    WriteResult ret(1);
    ret.timestamp_ = timestamp;
    return ret;
  }
  /// I/O error, for example if libhdf returned with a I/O error.
  ///
  /// \return The error result.
  static inline WriteResult ERROR_IO() { return WriteResult(-1); }

  static inline WriteResult ERROR_WITH_MESSAGE(std::string const &Message) {
    return WriteResult(Message);
  }

  /// \brief Indicates that the flatbuffer contained semantically invalid data,
  /// even
  /// though the flatbuffer is technically valid.
  ///
  /// The case that the flatbuffer itself is invalid should not occur as that
  /// is already checked for before passing the flatbuffer to the
  /// `HDFWriterModule`.
  ///
  /// \return The bad flatbuffer result.
  static inline WriteResult ERROR_BAD_FLATBUFFER() { return WriteResult(-2); }

  /// \brief Indicates that the data is structurally invalid, for example if it
  /// has the wrong array sizes.
  ///
  /// \return The data structure error result.
  static inline WriteResult ERROR_DATA_STRUCTURE_MISMATCH() {
    return WriteResult(-3);
  }

  /// \brief A special case of `ERROR_DATA_STRUCTURE_MISMATCH` to indicate that
  /// the data type does not match.
  ///
  /// For example a float instead of the expected
  /// double.
  ///
  /// \return The data type error result.
  static inline WriteResult ERROR_DATA_TYPE_MISMATCH() {
    return WriteResult(-4);
  }
  inline bool is_OK() const { return v == 0; }
  inline bool is_OK_WITH_TIMESTAMP() const { return v == 1; }

  /// \brief Indicates if any error has occurred.
  ///
  /// \return True if any error has occurred.
  inline bool is_ERR() const { return v < 0; }

  /// \brief Gets status reports.
  ///
  /// \return The status.
  std::string to_str() const;
  inline uint64_t timestamp() const { return timestamp_; }

private:
  explicit inline WriteResult(int8_t v) : v(v) {}
  explicit inline WriteResult(std::string Message)
      : v(-5), Message(std::move(Message)) {}
  int8_t v = -1;
  uint64_t timestamp_ = 0;
  std::string Message;
};
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
    auto FactoryFunction = []() {
      return std::unique_ptr<HDFWriterModule>(new Module());
    };
    addWriterModule(FlatbufferID, FactoryFunction);
  };
};
} // namespace HDFWriterModuleRegistry
} // namespace FileWriter
