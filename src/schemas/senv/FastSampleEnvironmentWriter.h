#pragma once

#include "../../FlatbufferReader.h"
#include "../../HDFFile.h"
#include "../../HDFWriterModule.h"
#include "../../Msg.h"
#include "Datasets.h"
#include "schemas/senv_data_generated.h"
#include <h5cpp/datatype/type_trait.hpp>


template<typename T> class ArrayAdapter
{
private:
  T *data_;
  size_t size_;
public:
  ArrayAdapter(T *data,size_t size) : data_(data), size_(size) {}
  size_t size() const {return size_;}
  T *data() {return data_;}
  const  T *data() const {return data_;}
};

namespace hdf5 {
namespace datatype {
  template <> class TypeTrait<std::uint16_t const> {
public:
  using Type = unsigned short const;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_UINT16)));
  }
};
  template <> class TypeTrait<std::uint64_t const> {
  public:
    using Type = unsigned short const;
    using TypeClass = Integer;
    static TypeClass create(const Type & = Type()) {
      return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_UINT64)));
    }
  };
  template <typename T>
  class TypeTrait<ArrayAdapter<T>> {
  public:
    using Type = ArrayAdapter<T>;
    using TypeClass = typename TypeTrait<T>::TypeClass;
    static TypeClass create(const Type & = Type()) {
      return TypeTrait<T>::create();
    }
  };
}
  namespace dataspace {
    template<typename T>
    class TypeTrait<ArrayAdapter<T>> {
    public:
      using DataspaceType = Simple;
      
      static DataspaceType create(const ArrayAdapter<T> &value) {
        return Simple(hdf5::Dimensions{value.size()}, hdf5::Dimensions{value.size()});
      }
      
      static void *ptr(ArrayAdapter<T> &data) {
        return reinterpret_cast<void *>(data.data());
      }
      
      static const void *cptr(const ArrayAdapter<T> &data) {
        return reinterpret_cast<const void *>(data.data());
      }
    };
  }
}

namespace senv {
using KafkaMessage = FileWriter::Msg;
using FBReaderBase = FileWriter::FlatbufferReader;
std::string nanoSecEpochToISO8601(std::uint64_t time);

class SampleEnvironmentDataGuard : public FBReaderBase {
public:
  bool verify(KafkaMessage const &Message) const override;
  std::string source_name(KafkaMessage const &Message) const override;
  uint64_t timestamp(KafkaMessage const &Message) const override;
};

using FileWriterBase = FileWriter::HDFWriterModule;
class FastSampleEnvironmentWriter : public FileWriterBase {
public:
  FastSampleEnvironmentWriter() = default;
  ~FastSampleEnvironmentWriter() = default;

  void parse_config(rapidjson::Value const &config_stream,
                    rapidjson::Value const *config_module) override;

  InitResult init_hdf(hdf5::node::Group &hdf_parent,
                      rapidjson::Value const *attributes) override;

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
