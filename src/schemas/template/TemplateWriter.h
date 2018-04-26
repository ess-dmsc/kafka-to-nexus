/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
///
/// \brief This file acts as a template for creating file writing modules.
///
/// All of the classes required are explained here. The
/// only thing missing from this header file is the registration of the file
/// writing module which must reside in an implementation file. See the
/// accompanying TestWriter.cpp for instructions on how this should be done.
///
/// A file writing module comprises two classes. The first class is the
/// flatbuffer reader which must implement the FileWriter::FlatbufferReader
/// interface. The virtual functions to be overridden are called to verify the
/// flatbuffer contents and extract a source name and a timestamp from the
/// flatbuffer. See the documentation of the individual member functions for
/// more information on how these should be implemented.
/// The second class which you must implement is a class that inherits from the
/// abstract class FileWriter::HDFWriterModule. This class should implement the
/// actual writing of flatbuffer data to the HDF5 file. More information on the
/// pure virtual functions that you must implement can be found below.

#pragma once
#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include <iostream>

/// \brief A separate namespace for this specific file writing module. Use this
/// to minimize the risk of name collisions.
namespace TemplateWriter {

/// \brief This class is used to extract information from a flatbuffer which
/// uses a specific four character file identifier.
///
/// See TestWriter.cpp for code which is used to tie a file identifier to an
/// instance of this class. Note that this class is only instantiated once in
/// the entire run of the application.
class ReaderClass : public FileWriter::FlatbufferReader {
public:
  /// \brief Is used to verify the contents of a flatbuffer.
  ///
  /// The `Verify*Buffer()` function available for the flatbuffer schema that
  /// you are using can be called here. This function check all offsets, sizes
  /// of fields, and null termination of strings to ensure that when a buffer
  /// is accessed, all reads will end up inside the buffer. Implementing
  /// additional checks, e.g. determining if a value falls with in an expected
  /// range, is also possible here.
  ///
  /// \note There are at least two good arguments for having this function not
  /// actually implement any verification of the data and return true by
  /// default: \li This member function is called on the data AFTER
  /// FileWriter::FlatbufferReader::source_name(), thus somewhat defeating the
  /// point of having it.
  /// \li Verifying a flatbuffer can (for some flatbuffer schemas) be relatively
  /// expensive and it is currently not possible to set the application to not
  /// call this function.
  ///
  /// The alternative is to do verification in the file writing part of the code
  /// i.e. FileWriter::HDFWriterModule::write().
  ///
  /// \note Try to avoid throwing any exceptions here as it (appears) to likely
  /// either crash the application or leave it in an inconsistent state.
  ///
  /// \param[in] Message The structure containing a pointer to a buffer
  /// containing data received from the Kafka broker and the size of the
  /// buffer.
  ///
  /// \return `true` if the data was verified as "correct", `false` otherwise.
  bool verify(FileWriter::Msg const &Message) const override {
    std::cout << "ReaderClass::verify()\n";
    return true;
  }

  /// \brief Extract the name of the data source from the flatbuffer.
  ///
  /// When setting up the file writer, a point (group) is tied to a specific
  /// module instance using the four character identifier of a file writing
  /// module and the name of the source of the data. This function is used to
  /// extract the name of a source given the relevant flatbuffer. Although the
  /// name can be as simple as a string contained in a flatbuffer, it might be
  /// appropriate in some cases to construct the name from (e.g.) a string and a
  /// channel number.
  ///
  /// \note In the current implementation, this member function might be called
  /// several times on the same flatbuffer.
  /// \note Try to avoid throwing any exceptions here as it (appears) to likely
  /// either crash the application or leave it in an inconsistent state.
  ///
  /// \param[in] Message The structure containing a pointer to a buffer
  /// containing data received from the Kafka broker and the size of the
  /// buffer.
  ///
  /// \return The name of the source of the data in the flatbuffer pointed to by
  /// the Message parameter.
  std::string source_name(FileWriter::Msg const &Message) const override {
    std::cout << "ReaderClass::source_name()\n";
    return "";
  }

  /// \brief Extract the timestamp of a flatbuffer.
  ///
  /// The file writer can be set to write data only in a specific "time window".
  /// Thus function is in that case used to extract the time of a flatbuffer and
  /// thus decide if its data should be written to file or not. Note that there
  /// are currently no specification or documentation as to which epoch is to
  /// be used nor unit of time and prefix. With that said, the current
  /// implementations appears to try to use (when possible) Unix epoch and
  /// nanoseconds.
  ///
  /// \note Try to avoid throwing any exceptions here as it (appears) to likely
  /// either crash the application or leave it in an inconsistent state.
  ///
  /// \param[in] Message The structure containing a pointer to a buffer
  /// containing data received from the Kafka broker and the size of the buffer.
  ///
  /// \return The timestamp of the flatbuffer as nanoseconds since Unix epoch
  /// (see above).
  uint64_t timestamp(FileWriter::Msg const &Message) const override {
    std::cout << "ReaderClass::timestamp()\n";
    return 0;
  }
};

/// \brief Implements the actual file writing code of this file writing module.
///
/// This class is instantiated twice for every new data source. First for
/// initialising the hdf-file (creating datasets etc.). The second instantiation
/// is used for writing the actual data. More information on this can be found
/// in the documentation for the member functions below.
class WriterClass : public FileWriter::HDFWriterModule {
public:
  /// \brief Constructor, initialise state here.
  ///
  /// \note Constructor is executed in a catch-all block (which re-throws)
  /// relatively high up in call hierarchy. You should for this reason probably
  /// try to avoid throwing exceptions here unless you encounter an
  /// unrecoverable state as any exceptions will cause the thread to exit.
  WriterClass() { std::cout << "WriterClass::WriterClass()\n"; }

  /// \brief Use to close datasets and return any other claimed resources.
  ///
  /// Use the destructor to close any open datasets and deallocate buffers etc.
  /// As mentioned previously, this class is instantiated twice for every data
  /// source. FileWriter::HDFWriterModule::close() is called only on the first
  /// instantiation. Thus if you have any resources that you allocate/claim in
  /// the constructor, you should probable return those here (the destructor)
  /// instead of in FileWriter::HDFWriterModule::close().
  ~WriterClass() override { std::cout << "WriterClass::~WriterClass()\n"; }

  /// \brief Used to pass configuration/settings to the current instance of this
  /// file writing module.
  ///
  /// Settings/configurations are passed in JSON form, contained in a
  /// rapidjson::Value object (one is unused, see the parameter documentation).
  /// To extract the information you want, you will need to implement JSON
  /// parsing code here. As this prototype does not have a return value and
  /// exceptions should not be thrown, the only way to inform the user of a
  /// non-fatal error is to write a log message (see logger.h"). The
  /// configurations are in the base of the JSON object and you should thus be
  /// able to extract relevant settings without navigating a JSON tree, unless
  /// the settings are by design in a tree structure. Examples of extracting
  /// settings from the JSON structure can be found in the files ev42_rw.cxx and
  /// f142_rw.cxx.
  ///
  /// \note This call is executed in a catch-all block (which re-throws)
  /// relatively high up in call hierarchy the first time it is called for a
  /// data source. You should for this reason probably try to avoid throwing
  /// exceptions here unless you encounter an unrecoverable state as any
  /// exceptions will cause the thread to exit.
  ///
  /// \param[in] config_stream Contains information about configurations
  /// relevant only to the current instance of this file writing module.
  /// \param[in] config_module This parameter is currently unused and thus any
  /// calls to this member function will have this parameter set to `nullptr`.
  void parse_config(rapidjson::Value const &config_stream,
                    rapidjson::Value const *config_module) override {
    std::cout << "WriterClass::parse_config()\n";
  }

  /// \brief Initialise datasets and attributes in the HDF5 file.
  ///
  /// This member function is used to initialise HDF groups, datasets,
  /// attributes etc. As SWMR (single writer, multiple reader) support in HDF5
  /// only allows for writing data pre-existing datasets, this
  /// function must implement the init functionality. A list of rules to follow
  /// when using SWMR can be found here:
  /// https://support.hdfgroup.org/HDF5/docNewFeatures/SWMR/HDF5_SWMR_Users_Guide.pdf.
  /// When initialising the HDF5 file, the call order (relevant to this class)
  /// is as follows:
  /// \li FileWriter::HDFWriterModule::HDFWriterModule()
  /// \li FileWriter::HDFWriterModule::parse_config()
  /// \li FileWriter::HDFWriterModule::init_hdf()
  /// \li FileWriter::HDFWriterModule::close()
  /// \li FileWriter::HDFWriterModule::~HDFWriterModule()
  ///
  /// \note This call is executed in a catch-all block (which re-throws)
  /// relatively high up in call hierarchy the first time it is called for a
  /// data source. You should for this reason probably try to avoid throwing
  /// exceptions here unless you encounter an unrecoverable state as any
  /// exceptions will cause the thread to exit.
  ///
  /// \note The return value of this function is not checked in the current
  /// implementation.
  ///
  /// \param[in] hdf_parent This is the HDF5 group where the relevant
  /// datasets should be created.
  /// \param[in] attributes There is no actual documentation on what this
  /// parameter contains but based on existing implementations, this
  /// rapidjson::Value instance contains attributes that you are responsible for
  /// writing to file. See ev42_rw.cxx and f142_rw.cxx for examples on how this
  /// can be done.
  ///
  /// \return An instance of InitResult. Note that these instances can only be
  /// constructed using the static methods InitResult::OK(),
  /// InitResult::ERROR_IO() and InitResult::ERROR_INCOMPLETE_CONFIGURATION().
  /// Note that the return value is not actually checked and thus returning an
  /// error has no side effects.
  InitResult init_hdf(hdf5::node::Group &hdf_parent,
                      rapidjson::Value const *attributes) override {
    std::cout << "WriterClass::init_hdf()\n";
    return InitResult::OK();
  }

  /// \brief Re-open datasets that have been created when calling
  /// FileWriter::HDFWriterModule::init_hdf().
  ///
  /// This function should not modify attributes, create datasets or a number of
  /// other things. See the following link for a list of things that you are not
  /// allowed to do when a file has been opened in SWMR mode:
  /// https://support.hdfgroup.org/HDF5/docNewFeatures/SWMR/HDF5_SWMR_Users_Guide.pdf.
  /// This member function is called in the second instantiation of this class
  /// (for a specific data source). The order in which methods (relevant to this
  /// class) are called is as follows:
  /// \li FileWriter::HDFWriterModule::HDFWriterModule()
  /// \li FileWriter::HDFWriterModule::parse_config()
  /// \li FileWriter::HDFWriterModule::reopen()
  /// \li Multiple calls to FileWriter::HDFWriterModule::write()
  /// \li FileWriter::HDFWriterModule::~HDFWriterModule()
  ///
  /// \note This call is executed in a catch-all block (which re-throws)
  /// relatively high up in call hierarchy the first time it is called for a
  /// data source. You should for this reason probably try to avoid throwing
  /// exceptions here unless you encounter an unrecoverable state as any
  /// exceptions will cause the thread to exit.
  ///
  /// \param[in] hdf_parent This is HDF5 group which has the datasets created
  /// using the call to init_hdf().
  ///
  /// \return An instance of InitResult. Note that these instances can only be
  /// constructed using the static methods InitResult::OK(),
  /// InitResult::ERROR_IO() and InitResult::ERROR_INCOMPLETE_CONFIGURATION().
  InitResult reopen(hdf5::node::Group &HDFGroup) override {
    std::cout << "WriterClass::reopen()\n";
    return InitResult::OK();
  }

  /// \brief Implements the data writing functionality of the file writing
  /// module.
  ///
  /// To properly support SWMR, only writes to datasets are allowed in this
  /// member function. See the following link for limitations when using a file
  /// in SWMR mode:
  /// https://support.hdfgroup.org/HDF5/docNewFeatures/SWMR/HDF5_SWMR_Users_Guide.pdf.
  /// This member function is called on the second instance of this class for a
  /// specific data source. The order in which methods (relevant to this class)
  /// are called is as follows:
  /// \li FileWriter::HDFWriterModule::HDFWriterModule()
  /// \li FileWriter::HDFWriterModule::parse_config()
  /// \li FileWriter::HDFWriterModule::reopen()
  /// \li Multiple calls to FileWriter::HDFWriterModule::write()
  /// \li FileWriter::HDFWriterModule::~HDFWriterModule()
  ///
  /// \note Try to avoid throwing any exceptions here as it (appears) to likely
  /// either crash the application or leave it in an inconsistent state.
  ///
  /// \param[in] Message The structure containing a pointer to a buffer
  /// containing data received from the Kafka broker and the size of the buffer.
  ///
  /// \return An instance of WriteResult. Note that these instances can only be
  /// constructed using a set of static member functions defined in
  /// HDFWriterModule.h. In the current implementation, returning a write result
  /// with a timestamp only has an effect on the log messages being generated.
  WriteResult write(FileWriter::Msg const &Message) override {
    std::cout << "WriterClass::write()\n";
    return WriteResult::OK();
  }

  /// \brief Provides no functionality and is never called.
  ///
  /// This member function is never called by the main application but because
  /// FileWriter::HDFWriterModule defines it as a pure virtual, it must be
  /// implemented in classes deriving from it.
  int32_t flush() override { return 0; }

  /// \brief Should (probably) not implement any functionality.
  ///
  /// In the current implementation, this member function is called only after
  /// FileWriter::HDFWriterModule::init_hdf() and nowhere else. Thus, this
  /// member function should not be trusted to be called and any cleanup should
  /// be done in the destructor instead (or as well).
  ///
  /// \note This call is executed in a catch-all block (which re-throws)
  /// relatively high up in call hierarchy. You should for this reason probably
  /// try to avoid throwing exceptions here unless you encounter an
  /// unrecoverable state as any exceptions will cause the thread to exit.
  ///
  /// \return The return value is never checked. For parity with other file
  /// writing modules, return 0.
  int32_t close() override {
    std::cout << "WriterClass::close()\n";
    return 0;
  }

  /// \brief Provides no functionality and is never called.
  ///
  /// This member function is never called by the main application but because
  /// FileWriter::HDFWriterModule defines it as a pure virtual, it must be
  /// implemented in classes deriving from it.
  void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                 int mpi_rank) override {}
};
} // namespace TemplateWriter
