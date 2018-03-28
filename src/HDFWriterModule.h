#pragma once

#include "CollectiveQueue.h"
#include "Msg.h"
#include <fmt/format.h>
#include <functional>
#include <h5cpp/hdf5.hpp>
#include <map>
#include <memory>
#include <rapidjson/document.h>
#include <string>

namespace FileWriter {

namespace HDFWriterModule_detail {

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
  inline bool is_OK() { return v == 0; }

  /// Indicates if any error has occurred. More specific query function will
  /// come as need arises.
  ///
  /// \return True if any error has occurred
  inline bool is_ERR() { return v < 0; }

  /// Used for status reports.
  ///
  /// \return The status.
  std::string to_str() const;

private:
  explicit inline InitResult(int8_t v) : v(v) {}
  int8_t v = -1;
};

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

  /// Indicates that the flatbuffer contained semantically invalid data, even
  /// though the flatbuffer is technically valid.
  ///
  /// The case that the flatbuffer itself is invalid should not occur as that
  /// is already checked for before passing the flatbuffer to the
  /// `HDFWriterModule`.
  ///
  /// \return The bad flatbuffer result.
  static inline WriteResult ERROR_BAD_FLATBUFFER() { return WriteResult(-2); }

  /// Indicates that the data is structurally invalid, for example if it has
  /// the wrong array sizes.
  ///
  /// \return The data structure error result.
  static inline WriteResult ERROR_DATA_STRUCTURE_MISMATCH() {
    return WriteResult(-3);
  }

  /// A special case of `ERROR_DATA_STRUCTURE_MISMATCH` to indicate that
  /// the data type does not match, for example a float instead of the expected
  /// double.
  ///
  /// \return The data type error result.
  static inline WriteResult ERROR_DATA_TYPE_MISMATCH() {
    return WriteResult(-4);
  }
  inline bool is_OK() { return v == 0; }
  inline bool is_OK_WITH_TIMESTAMP() { return v == 1; }

  /// Indicates if any error has occurred. More specific query function will
  /// come as need arises.
  ///
  /// \return True if any error has occurred.
  inline bool is_ERR() { return v < 0; }

  /// Used for status reports.
  ///
  /// \return The status.
  std::string to_str() const;
  inline uint64_t timestamp() const { return timestamp_; }

private:
  explicit inline WriteResult(int8_t v) : v(v) {}
  int8_t v = -1;
  uint64_t timestamp_ = 0;
};
} // namespace HDFWriterModule_detail

/// Writes a given flatbuffer to HDF.
///
/// Base class for the writer modules which are responsible for actually
/// writing a flatbuffer message to the HDF file.  A HDFWriterModule is
/// instantiated for each 'stream' which is configured in a file writer json
/// command.  The HDFWriterModule class registers itself via a string id which
/// must be unique.  This id is used in the file writer json command.  The id
/// can be arbitrary but should as a convention contain the flatbuffer schema
/// id (`FBID`) like `FBID_<writer-module-name>`.
/// Example: Please see `src/schemas/ev42/ev42_rw.cxx`.
class HDFWriterModule {
public:
  using ptr = std::unique_ptr<HDFWriterModule>;
  using InitResult = HDFWriterModule_detail::InitResult;
  using WriteResult = HDFWriterModule_detail::WriteResult;

  virtual ~HDFWriterModule() = default;

  /// Parses the configuration of a stream.
  ///
  /// \param config_stream Configuration from the write file command for this
  /// stream.
  /// \param config_module Configuration for all instances of this
  /// HDFWriterModule.
  virtual void parse_config(rapidjson::Value const &config_stream,
                            rapidjson::Value const *config_module) = 0;

  /// Initialise the HDF file.
  ///
  /// Called before any data has arrived with the json configuration of this
  /// stream to allow the `HDFWriterModule` to create any structures in the HDF
  /// file.
  ///
  /// \param hdf_file The HDF file handle.
  /// \param hdf_parent_name Path to the group into which this HDFWriterModule
  /// should put its data.
  /// \return The result.
  virtual InitResult init_hdf(hdf5::node::Group &hdf_parent,
                              std::string hdf_parent_name,
                              rapidjson::Value const *attributes,
                              CollectiveQueue *cq) = 0;

  virtual InitResult reopen(hdf5::node::Group hdf_file,
                            std::string hdf_parent_name, CollectiveQueue *cq,
                            HDFIDStore *hdf_store) = 0;

  /// Process the message in some way, for example write to the HDF file.
  ///
  /// \param msg The message to process
  /// \return The result.
  virtual WriteResult write(Msg const &msg) = 0;

  /// Flush the internal buffer.
  ///
  /// You are expected to flush all the internal buffers which you have to
  /// the HDF file.
  ///
  /// \return Error code.
  virtual int32_t flush() = 0;

  /// Close all open HDF handles, datasets, dataspaces, groups,
  /// everything.
  ///
  /// \return Error code.
  virtual int32_t close() = 0;

  virtual void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                         int mpi_rank) = 0;
};

/// Keeps track of the registered FlatbufferReader instances.
///
/// See for example `src/schemas/ev42/ev42_rw.cxx` and search for
/// HDFWriterModuleRegistry.
namespace HDFWriterModuleRegistry {
using Key = std::string;
using Value = std::function<std::unique_ptr<HDFWriterModule>()>;
std::map<Key, Value> &items();
Value &find(Key const &key);

void registrate(Key key, Value value);

class Registrar {
public:
  Registrar(Key key, Value value) {
    HDFWriterModuleRegistry::registrate(key, value);
  }
};
} // namespace HDFWriterModuleRegistry
} // namespace FileWriter
