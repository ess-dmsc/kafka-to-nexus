#pragma once

#include "../../HDFFile.h"
#include "../../Msg.h"
#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include "schemas/senv_data_generated.h"

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
                                std::string hdf_parent_name,
                                rapidjson::Value const *attributes,
                                CollectiveQueue *cq) override;
    
    InitResult reopen(hdf5::node::Group hdf_file,
                              std::string hdf_parent_name, CollectiveQueue *cq,
                              HDFIDStore *hdf_store) override;
    
    WriteResult write(KafkaMessage const &Message) override;
    
    int32_t flush() override;
    
    int32_t close() override;
    
    void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                   int mpi_rank) override;
  protected:
    
  };
} //namespace senv
