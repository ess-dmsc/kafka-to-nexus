#pragma once

#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include "../../h5.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename T> using uptr = std::unique_ptr<T>;

class Writer : public FileWriter::HDFWriterModule {
public:
  static FileWriter::HDFWriterModule::ptr create();
  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override;
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  InitResult reopen(hdf5::node::Group &HDFGroup) override;
  WriteResult write(FlatbufferMessage const &Message) override;
  int32_t flush() override;
  int32_t close() override;
  void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store, int mpi_rank) {}

  hsize_t ChunkBytes = 1 << 18;
  bool DoFlushEachWrite = false;
  uint64_t TotalWrittenBytes = 0;
};

std::runtime_error unimplemented() {
  exit(200);
  return std::runtime_error("unimplemented");
}
}
}
}
