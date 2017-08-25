#pragma once

#include "Msg.h"
#include <H5Ipublic.h>
#include <fmt/format.h>
#include <functional>
#include <map>
#include <memory>
#include <rapidjson/document.h>
#include <string>

namespace FileWriter {

namespace HDFWriterModule_detail {

class InitResult {
public:
  /// Everything was fine.
  static InitResult OK() { return InitResult(0); }
  /// I/O error, for example if libhdf returned with a I/O error.
  static inline InitResult ERROR_IO() { return InitResult(-1); }
  /// The writer module needs more configuration that was is available.
  static inline InitResult ERROR_INCOMPLETE_CONFIGURATION() {
    return InitResult(-2);
  }
  inline bool is_OK() { return v == 0; }
  /// `true` if any error has occurred. More specific query function will come
  /// as need arises.
  inline bool is_ERR() { return v < 0; }
  /// Used for status reports.
  std::string to_str() const;

private:
  explicit inline InitResult(uint64_t v) : v(v) {}
  int64_t v = -1;
};

class WriteResult {
public:
  /// Everything was fine.
  static WriteResult OK() { return WriteResult(0); }
  /// I/O error, for example if libhdf returned with a I/O error.
  static inline WriteResult ERROR_IO() { return WriteResult(-1); }
  /// Indicates that the flatbuffer contained semantically invalid data, even
  /// though the flatbuffer is technically valid.
  /// The case that the flatbuffer itself is invalid should not occur as that
  /// is already checked for before passing the flatbuffer to the
  /// `HDFWriterModule`.
  static inline WriteResult ERROR_BAD_FLATBUFFER() { return WriteResult(-2); }
  /// Indicates that the data is structurally invalid, for example if it has
  /// the wrong array sizes.
  static inline WriteResult ERROR_DATA_STRUCTURE_MISMATCH() {
    return WriteResult(-3);
  }
  /// More a special case of `ERROR_DATA_STRUCTURE_MISMATCH` to indicate that
  /// the data type does not match, for example a float instead of the expected
  /// double.
  static inline WriteResult ERROR_DATA_TYPE_MISMATCH() {
    return WriteResult(-4);
  }
  inline bool is_OK() { return v == 0; }
  /// `true` if any error has occurred. More specific query function will come
  /// as need arises.
  inline bool is_ERR() { return v < 0; }
  /// Used for status reports.
  std::string to_str() const;

private:
  explicit inline WriteResult(uint64_t v) : v(v) {}
  int64_t v = -1;
};
}

/// \brief Writes a given flatbuffer to HDF.

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
  typedef std::unique_ptr<HDFWriterModule> ptr;
  typedef HDFWriterModule_detail::InitResult InitResult;
  typedef HDFWriterModule_detail::WriteResult WriteResult;

  static std::unique_ptr<HDFWriterModule> create();

  virtual ~HDFWriterModule();

  /// Called before any data has arrived with the json configuration of this
  /// stream to allow the `HDFWriterModule` to create any structures in the HDF
  /// file.
  /// @param hid HDF object id into which the HDFWriterModule should put its
  /// data.
  /// @param config_stream Configuration from the write file command for this
  /// stream.
  /// @param config_module Configuration for all instances of this
  /// HDFWriterModule.
  virtual InitResult init_hdf(hid_t hid, rapidjson::Value const &config_stream,
                              rapidjson::Value const *config_module) = 0;

  /// Process the message in some way, for example write to the HDF file.
  virtual WriteResult write(Msg const &msg) = 0;

  // You are expected to flush all your internal buffers which you may have to
  // the HDF file.
  virtual int32_t flush() = 0;

  // Please close all open HDF handles, datasets, dataspaces, groups,
  // everything.
  virtual int32_t close() = 0;
};

/// \brief Keeps track of the registered FlatbufferReader instances.

/// See for example `src/schemas/ev42/ev42_rw.cxx` and search for
/// HDFWriterModuleRegistry.

class HDFWriterModuleRegistry {
public:
  typedef std::string K;
  typedef std::function<std::unique_ptr<HDFWriterModule>()> V;
  static std::map<K, V> &items();
  static V &find(K const &key);

  static void registrate(K key, V value) {
    auto &m = items();
    if (m.find(key) != m.end()) {
      auto s = fmt::format("ERROR entry for key [{}] exists already", key);
      throw std::runtime_error(s);
    }
    m[key] = std::move(value);
  }

  class Registrar {
  public:
    Registrar(K key, V value) {
      HDFWriterModuleRegistry::registrate(key, value);
    }
  };
};
}
