#include "../../HDFWriterModule.h"
#include "../../h5.h"
#include "Common.h"
#include "WriterArray.h"
#include "WriterScalar.h"
#include "WriterTypedBase.h"
#include <array>
#include <chrono>
#include <memory>
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace f142 {

enum class CreateWriterTypedBaseMethod { CREATE, OPEN };

struct DatasetInfo {
  std::string Name;
  size_t ChunkBytes;
  size_t BufferSize;
  size_t BufferPacketMaxSize;
  uptr<h5::h5d_chunked_1d<uint64_t>> *Ptr;
};

class HDFWriterModule final : public FileWriter::HDFWriterModule {
public:
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override;
  HDFWriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const *HDFAttributes,
                      CreateWriterTypedBaseMethod CreateMethod);
  void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                 int mpi_rank) override;
  WriteResult write(Msg const &msg) override;
  int32_t flush() override;
  int32_t close() override;
  HDFWriterModule();
  ~HDFWriterModule() {}

  uptr<WriterTypedBase> impl;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_timestamp;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_cue_timestamp_zero;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_cue_index;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_seq_data;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_seq_fwd;
  uptr<h5::h5d_chunked_1d<uint64_t>> ds_ts_data;
  std::vector<DatasetInfo> DatasetInfoList;
  bool do_flush_always = false;
  bool DoWriteForwarderInternalDebugInfo = false;
  uint64_t WrittenBytesTotal = 0;
  uint64_t IndexAtBytes = 0;
  // set by default to a large value:
  uint64_t index_every_bytes = !0;
  uint64_t ts_max = 0;
  size_t ArraySize = 0;
  std::string SourceName;
  std::string TypeName;
  CollectiveQueue *cq = nullptr;
  using CLOCK = std::chrono::steady_clock;
  using MS = std::chrono::milliseconds;
  MS ErrorLogMinInterval{500};
  std::chrono::time_point<CLOCK> TimestampLastErrorLog{CLOCK::now() -
                                                       ErrorLogMinInterval};
};

struct WriterFactory {
  virtual std::unique_ptr<WriterTypedBase>
  createWriter(hdf5::node::Group Group, std::string Name, size_t Columns,
               FileWriter::Schemas::f142::Value ValueUnionID,
               CollectiveQueue *cq) = 0;
  virtual std::unique_ptr<WriterTypedBase>
  createWriter(hdf5::node::Group Group, std::string Name, size_t Columns,
               FileWriter::Schemas::f142::Value ValueUnionID,
               CollectiveQueue *cq, HDFIDStore *HDFStore) = 0;
  virtual FileWriter::Schemas::f142::Value getValueUnionID() = 0;
};

template <typename C_TYPE, typename FB_VALUE_TYPE>
struct WriterFactoryScalar : public WriterFactory {
  FileWriter::Schemas::f142::Value ValueUnionID =
      ValueTraits<FB_VALUE_TYPE>::enum_value;

  std::unique_ptr<WriterTypedBase>
  createWriter(hdf5::node::Group Group, std::string Name, size_t Columns,
               FileWriter::Schemas::f142::Value ValueUnionID,
               CollectiveQueue *cq) override {
    return std::unique_ptr<WriterTypedBase>(
        new WriterScalar<C_TYPE, FB_VALUE_TYPE>(Group, Name, ValueUnionID, cq));
  }

  std::unique_ptr<WriterTypedBase>
  createWriter(hdf5::node::Group Group, std::string Name, size_t Columns,
               FileWriter::Schemas::f142::Value ValueUnionID,
               CollectiveQueue *cq, HDFIDStore *HDFStore) override {
    return std::unique_ptr<WriterTypedBase>(
        new WriterScalar<C_TYPE, FB_VALUE_TYPE>(Group, Name, ValueUnionID, cq,
                                                HDFStore));
  }

  FileWriter::Schemas::f142::Value getValueUnionID() override {
    return ValueUnionID;
  }
};

template <typename C_TYPE, typename FB_VALUE_TYPE>
struct WriterFactoryArray : public WriterFactory {
  FileWriter::Schemas::f142::Value ValueUnionID =
      ValueTraits<FB_VALUE_TYPE>::enum_value;

  std::unique_ptr<WriterTypedBase>
  createWriter(hdf5::node::Group Group, std::string Name, size_t Columns,
               FileWriter::Schemas::f142::Value ValueUnionID,
               CollectiveQueue *cq) override {
    return std::unique_ptr<WriterTypedBase>(
        new WriterArray<C_TYPE, FB_VALUE_TYPE>(Group, Name, Columns,
                                               ValueUnionID, cq));
  }

  std::unique_ptr<WriterTypedBase>
  createWriter(hdf5::node::Group Group, std::string Name, size_t Columns,
               FileWriter::Schemas::f142::Value ValueUnionID,
               CollectiveQueue *cq, HDFIDStore *HDFStore) override {
    return std::unique_ptr<WriterTypedBase>(
        new WriterArray<C_TYPE, FB_VALUE_TYPE>(Group, Name, Columns,
                                               ValueUnionID, cq, HDFStore));
  }

  FileWriter::Schemas::f142::Value getValueUnionID() override {
    return ValueUnionID;
  }
};
}
}
}
