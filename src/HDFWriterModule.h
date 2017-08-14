#pragma once

namespace FileWriter {

/// Base class for the writer modules which are responsible for actually
/// writing a flatbuffer message to the HDF file.  A HDFWriterModule is
/// instantiated for each 'stream' which is configured in a file writer json
/// command.  The HDFWriterModule class registers itself via a string id which
/// must be unique.  This id is used in the file writer json command.

class HDFWriterModule {
public:
  typedef std::unique_ptr<HDFWriterModule> ptr;
  static std::unique_ptr<HDFWriterModule> create();
  virtual ~HDFWriterModule();

  /// Called before any data has arrived with the json configuration of this
  /// stream to allow the `HDFWriterModule` to create any structures in the HDF
  /// file.
  /// TODO
  /// - Possible return types?
  ///   OK, MISSING_CONFIG(string), COULD_NOT, IOERROR
  /**
  Help text for `config_stream`:
  Contains all the options set for the stream that this FBSchemaWriter
  should write to HDF.  If your plugin needs to be configurable, this is where
  you can access the options.
  */
  virtual int init_hdf(hid_t hid, rapidjson::Value const &config_stream,
                       rapidjson::Value const &config_file) = 0;

  /// Process the message in some way, for example write to the HDF file.
  /// TODO
  /// - More possible return types
  ///   OK, IOERROR, CORRUPT_FLATBUFFER, UNEXPECTED_DATA
  virtual WriteResult write(Msg const &msg) = 0;

  // TODO return values?
  // You are expected to flush all your internal buffers which you may have to
  // the HDF file.
  virtual int flush() = 0;

  // TODO return values?
  // Please close all open HDF handles, datasets, dataspaces, groups,
  // everything.
  virtual int close() = 0;
};
}
