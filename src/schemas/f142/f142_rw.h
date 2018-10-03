#include "../../HDFWriterModule.h"
#include "../../h5.h"
#include "Common.h"
#include "WriterArray.h"
#include "WriterScalar.h"
#include "WriterScalarString.h"
#include "WriterTypedBase.h"
#include <array>
#include <chrono>
#include <memory>
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// Because of SWMR, we have to create HDF structures first, then close
/// everything again, and then reopen those structures from the previously
/// created file.  This enum is used to indicate which phase we are in.
enum class CreateWriterTypedBaseMethod { CREATE, OPEN };

/// The HDFWriterModule contains a list of these DatasetInfo, which represents
/// the essential information about the datasets used in this HDFWriterModule.
/// By having this information in a separate list we can condense the code
/// needed for initialization.
struct DatasetInfo {
  /// Name of the HDF dataset
  std::string Name;
  /// Size of the HDF chunks in bytes
  size_t ChunkBytes;
  /// Size of the buffer which is used to optimize writing of small messages
  size_t BufferSize;
  /// Maximum size of a message which is considered for buffering
  size_t BufferPacketMaxSize;
  /// Helper
  uptr<h5::h5d_chunked_1d<uint64_t>> &Ptr;
  DatasetInfo(std::string Name, size_t ChunkBytes, size_t BufferSize,
              size_t BufferPacketMaxSize,
              uptr<h5::h5d_chunked_1d<uint64_t>> &Ptr);
};

class HDFWriterModule final : public FileWriter::HDFWriterModule {
public:
  /// Implements HDFWriterModule interface
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  /// Implements HDFWriterModule interface
  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override;
  /// Implements HDFWriterModule interface
  HDFWriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;

  /// Actual initialziation code, mostly shared among CREATE and OPEN phases.
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const *HDFAttributes,
                      CreateWriterTypedBaseMethod CreateMethod);

  /// Write an incoming message which should contain a flatbuffer
  WriteResult write(FlatbufferMessage const &Message) override;

  /// Flush underlying buffers
  int32_t flush() override;
  int32_t close() override;

  HDFWriterModule();
  ~HDFWriterModule() override = default;

  /// Datatype as given in the filewriter json command
  std::string TypeName;

  /// Name of the source that we write
  std::string SourceName;

  /// Dataset with the actual values, with requested datatype
  uptr<WriterTypedBase> ValueWriter;

  /// Timestamps of the f142 updates
  uptr<h5::h5d_chunked_1d<uint64_t>> DatasetTimestamp;

  /// Index into DatasetTimestamp
  uptr<h5::h5d_chunked_1d<uint64_t>> DatasetCueTimestampZero;

  /// Index into the f142 values
  uptr<h5::h5d_chunked_1d<uint64_t>> DatasetCueIndex;

  // Some optional helper datasets for debugging
  bool DoWriteForwarderInternalDebugInfo = false;
  uptr<h5::h5d_chunked_1d<uint64_t>> DatasetSeqData;
  uptr<h5::h5d_chunked_1d<uint64_t>> DatasetSeqFwd;
  uptr<h5::h5d_chunked_1d<uint64_t>> DatasetTsData;

  /// List of essential information about the datasets used by this
  /// HDFModuleWriter.  Used in `init_hdf()`
  std::vector<DatasetInfo> DatasetInfoList;

  uint64_t WrittenBytesTotal = 0;
  uint64_t IndexAtBytes = 0;
  // set by default to a large value:
  uint64_t IndexEveryBytes = std::numeric_limits<uint64_t>::max();
  uint64_t TimestampMax = 0;
  size_t ArraySize = 0;

  // Helper for experiments on my other branch
  void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                 int mpi_rank) override;
  CollectiveQueue *cq = nullptr;

  // Reduce LOG rate in some cases
  using CLOCK = std::chrono::steady_clock;
  using MS = std::chrono::milliseconds;
  MS ErrorLogMinInterval{500};
  std::chrono::time_point<CLOCK> TimestampLastErrorLog{CLOCK::now() -
                                                       ErrorLogMinInterval};
};

/// \brief  Interface for creating and opening a dataset
///
/// Interface for creating and opening a dataset for the f142 values of a
/// dynamically specified datatype.  Implemented by WriterFactoryScalar and
/// WriterFactoryArray.
struct WriterFactory {
  virtual ~WriterFactory() = default;
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

/// \brief  Factory for scalar writers
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

/// \brief  Factory for array writers
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

/// \brief  Factory for scalar string writers
struct WriterFactoryScalarString : public WriterFactory {
  FileWriter::Schemas::f142::Value ValueUnionID =
      ValueTraits<String>::enum_value;

  std::unique_ptr<WriterTypedBase>
  createWriter(hdf5::node::Group Group, std::string Name, size_t Columns,
               FileWriter::Schemas::f142::Value ValueUnionID,
               CollectiveQueue *cq) override {
    return std::unique_ptr<WriterTypedBase>(
        new WriterScalarString(Group, Name, ValueUnionID, cq));
  }

  std::unique_ptr<WriterTypedBase>
  createWriter(hdf5::node::Group Group, std::string Name, size_t Columns,
               FileWriter::Schemas::f142::Value ValueUnionID,
               CollectiveQueue *cq, HDFIDStore *HDFStore) override {
    return std::unique_ptr<WriterTypedBase>(
        new WriterScalarString(Group, Name, ValueUnionID, cq, HDFStore));
  }

  FileWriter::Schemas::f142::Value getValueUnionID() override {
    return ValueUnionID;
  }
};
}
}
}
