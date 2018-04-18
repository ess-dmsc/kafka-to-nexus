#pragma once

#include "../../FlatbufferReader.h"
#include "../../HDFFile.h"
#include "../../HDFWriterModule.h"
#include "../../Msg.h"
#include "Datasets.h"
#include "gsl/span"
#include "schemas/senv_data_generated.h"
#include <h5cpp/datatype/type_trait.hpp>

namespace hdf5 {
namespace datatype {
template <> class TypeTrait<unsigned short const> {
public:
  using Type = unsigned short const;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_USHORT)));
  }
};

template <typename T> class TypeTrait<gsl::span<T>> {
public:
  using Type = gsl::span<T>;
  using TypeClass = typename TypeTrait<T>::TypeClass;
  static TypeClass create(const Type & = Type()) {
    return TypeTrait<T>::create();
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
