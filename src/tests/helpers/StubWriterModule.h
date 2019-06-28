#pragma once
#include "HDFWriterModule.h"
#include "FlatbufferMessage.h"

class StubWriterModule : public FileWriter::HDFWriterModule {
public:
  void parse_config(std::string const &/*ConfigurationStream*/,
                    std::string const &/*ConfigurationModule*/) override {}
  InitResult init_hdf(hdf5::node::Group &/*HDFGroup*/,
                      std::string const &/*HDFAttributes*/) override {
    return InitResult::OK;
  }
  InitResult reopen(hdf5::node::Group &/*HDFGroup*/) override {
    return InitResult::OK;
  }
  void write(FileWriter::FlatbufferMessage const &/*Message*/) override {}
  std::int32_t flush() override { return 0; }
  std::int32_t close() override { return 0; }
};

