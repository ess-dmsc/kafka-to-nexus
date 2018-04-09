

#include "../../helper.h"

#include <limits>
#include <iostream>

#include "FastSampleEnvironmentWriter.h"

namespace senv {
  using WriterFactory = FileWriter::HDFWriterModuleRegistry::Value;
  using WriterRegistrator = FileWriter::HDFWriterModuleRegistry::Registrar;
  
  std::unique_ptr<FileWriterBase> InstantiateWriter() {
    return std::unique_ptr<FileWriterBase>(new FastSampleEnvironmentWriter());
  };
  
  WriterRegistrator RegisteredWriter("senv", InstantiateWriter);
  
  using WriteGuardRegistrator = FileWriter::FlatbufferReaderRegistry::Registrar<SampleEnvironmentDataGuard>;
  
  WriteGuardRegistrator RegisteredGuard(std::array<char,4>{{'s', 'e', 'n', 'v'}});

  
  std::string nanoSecEpochToISO8601(std::uint64_t time) {
    time_t secondsPart = time / 1000000000;
    tm *tmPtr = gmtime(&secondsPart);
    std::uint64_t nSecPart = time - std::uint64_t(secondsPart * 1000000000);
    double printedSecs = double(nSecPart) / 1000000000 + tmPtr->tm_sec;
    std::string formatString = "{0:>4d}-{1:0>2d}-{2:0>2d}T{3:0>2d}:{4:0>2d}:{5:0>9.6f}Z";
    return fmt::format(formatString, tmPtr->tm_year + 1900, tmPtr->tm_mon + 1, tmPtr->tm_mday, tmPtr->tm_hour, tmPtr->tm_min, printedSecs);
  }
  
  bool SampleEnvironmentDataGuard::verify(KafkaMessage const &Message) const {
    auto Verifier = flatbuffers::Verifier((uint8_t *)Message.data(), Message.size());
    if (VerifySampleEnvironmentDataBuffer(Verifier))
      return true;
    return false;
  }
  
  uint64_t SampleEnvironmentDataGuard::timestamp(KafkaMessage const &Message) const {
    auto FbPointer = GetSampleEnvironmentData(Message.data());
    /// \todo This timestamp is currently EPICS epoch. This will have to be sorted out.
    return FbPointer->PacketTimestamp();
  }

  std::string SampleEnvironmentDataGuard::source_name(const KafkaMessage &Message) const {
    auto FbPointer = GetSampleEnvironmentData(Message.data());
    return FbPointer->Name()->str();
  }
  
  void FastSampleEnvironmentWriter::parse_config(const rapidjson::Value &config_stream, const rapidjson::Value *config_module) {
    LOG(Sev::Debug, "There are currently no runtime configurable options in the FastSampleEnvironmentWriter class.");
  }
  
  FileWriterBase::InitResult FastSampleEnvironmentWriter::init_hdf(hdf5::node::Group &hdf_parent,
                      std::string hdf_parent_name,
                      rapidjson::Value const *attributes,
                                                   CollectiveQueue *cq) {
    return FileWriterBase::InitResult::OK();
  }
  
  FileWriterBase::InitResult FastSampleEnvironmentWriter::reopen(hdf5::node::Group hdf_file, std::string hdf_parent_name, CollectiveQueue *cq, HDFIDStore *hdf_store) {
    return FileWriterBase::InitResult::OK();
  }
  
  FileWriterBase::WriteResult FastSampleEnvironmentWriter::write(const KafkaMessage &Message) {
    return FileWriterBase::WriteResult::OK();
  }
  
  std::int32_t FastSampleEnvironmentWriter::flush() {
    LOG(Sev::Error, "Flush not implemented.");
    return 0;
  }
  
  std::int32_t FastSampleEnvironmentWriter::close() {
    return 0;
  }
  
  void FastSampleEnvironmentWriter::enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store, int mpi_rank) {
    LOG(Sev::Error, "Collective queue not implemented.");
  }
  
} // namespace senv
