/** Copyright (C) 2018 European Spallation Source ERIC */

/** \file
 *
 *  \brief Define classes required to implement the ADC file writing module.
 */

#pragma once

#include "../../FlatbufferReader.h"
#include "../../HDFFile.h"
#include "../../HDFWriterModule.h"
#include "../../Msg.h"
#include "Datasets.h"
#include "schemas/senv_data_generated.h"
#include <h5cpp/datatype/type_trait.hpp>

/// \todo The following helper classes are used to interface with the h5cpp
/// library. They must be removed from here when they are added to the h5cpp
/// library.

/// \brief Used to write c-arrays to hdf5 files using h5cpp.
///
/// The member functions of this class need no extra documentation.
template <typename T> class ArrayAdapter {
public:
  ArrayAdapter(T *data, size_t size) : data_(data), size_(size) {}
  size_t size() const { return size_; }
  T *data() { return data_; }
  const T *data() const { return data_; }

private:
  T *data_;
  size_t size_;
};

namespace hdf5 {
namespace datatype {
// \brief Required for h5cpp to write const unsigned short data.
template <> class TypeTrait<std::uint16_t const> {
public:
  using Type = unsigned short const;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_UINT16)));
  }
};

// \brief Required for h5cpp to write const unsigned long long int data.
template <> class TypeTrait<std::uint64_t const> {
public:
  using Type = unsigned short const;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_UINT64)));
  }
};

// \brief Required for h5cpp to write data provided using ArrayAdapter.
template <typename T> class TypeTrait<ArrayAdapter<T>> {
public:
  using Type = ArrayAdapter<T>;
  using TypeClass = typename TypeTrait<T>::TypeClass;
  static TypeClass create(const Type & = Type()) {
    return TypeTrait<T>::create();
  }
};
} // namespace datatype
namespace dataspace {

// \brief Required for h5cpp to write data provided using ArrayAdapter.
template <typename T> class TypeTrait<ArrayAdapter<T>> {
public:
  using DataspaceType = Simple;

  static DataspaceType create(const ArrayAdapter<T> &value) {
    return Simple(hdf5::Dimensions{value.size()},
                  hdf5::Dimensions{value.size()});
  }

  static void *ptr(ArrayAdapter<T> &data) {
    return reinterpret_cast<void *>(data.data());
  }

  static const void *cptr(const ArrayAdapter<T> &data) {
    return reinterpret_cast<const void *>(data.data());
  }
};
} // namspace dataspace
} // namespace hdf5

namespace senv {
using KafkaMessage = FileWriter::Msg;
using FBReaderBase = FileWriter::FlatbufferReader;

/// \brief See parent class for documentation.
class SampleEnvironmentDataGuard : public FBReaderBase {
public:
  bool verify(KafkaMessage const &Message) const override;
  std::string source_name(KafkaMessage const &Message) const override;
  uint64_t timestamp(KafkaMessage const &Message) const override;
};

using FileWriterBase = FileWriter::HDFWriterModule;

std::vector<std::uint64_t> GenerateTimeStamps(std::uint64_t OriginTimeStamp,
                                              double TimeDelta,
                                              int NumberOfElements);

/// \brief See parent class for documentation.
class FastSampleEnvironmentWriter : public FileWriterBase {
public:
  FastSampleEnvironmentWriter() = default;
  ~FastSampleEnvironmentWriter() = default;

  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override;

  WriteResult write(KafkaMessage const &Message) override;

  int32_t flush() override;

  int32_t close() override;

  void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                 int mpi_rank) override;

protected:
  NeXusDataset::RawValue Value;
  NeXusDataset::Time Timestamp;
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;
};
} // namespace senv
