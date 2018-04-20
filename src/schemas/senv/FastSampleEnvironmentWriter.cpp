

#include "../../helper.h"

#include <iostream>
#include <limits>

#include "FastSampleEnvironmentWriter.h"

namespace senv {

FileWriter::FlatbufferReaderRegistry::Registrar<SampleEnvironmentDataGuard>
    RegisterSenvGuard("senv");

FileWriter::HDFWriterModuleRegistry::Registrar<FastSampleEnvironmentWriter>
    RegisterSenvWriter("senv");

bool SampleEnvironmentDataGuard::verify(KafkaMessage const &Message) const {
  auto Verifier =
      flatbuffers::Verifier((uint8_t *)Message.data(), Message.size());
  if (VerifySampleEnvironmentDataBuffer(Verifier))
    return true;
  return false;
}

uint64_t
SampleEnvironmentDataGuard::timestamp(KafkaMessage const &Message) const {
  auto FbPointer = GetSampleEnvironmentData(Message.data());
  /// \todo This timestamp is currently EPICS epoch. This will have to be sorted
  /// out.
  return FbPointer->PacketTimestamp();
}

std::string
SampleEnvironmentDataGuard::source_name(const KafkaMessage &Message) const {
  auto FbPointer = GetSampleEnvironmentData(Message.data());
  return FbPointer->Name()->str();
}

void FastSampleEnvironmentWriter::parse_config(
    const rapidjson::Value &config_stream,
    const rapidjson::Value *config_module) {
  LOG(Sev::Debug, "There are currently no runtime configurable options in the "
                  "FastSampleEnvironmentWriter class.");
}

FileWriterBase::InitResult
FastSampleEnvironmentWriter::init_hdf(hdf5::node::Group &HDFGroup,
                                      rapidjson::Value const *attributes) {
  const int DefaultChunkSize = 1024;
  try {
    auto &CurrentGroup = HDFGroup;
    NeXusDataset::RawValue(CurrentGroup, DefaultChunkSize);
    NeXusDataset::Time(CurrentGroup, DefaultChunkSize);
    NeXusDataset::CueIndex(CurrentGroup, DefaultChunkSize);
    NeXusDataset::CueTimestampZero(CurrentGroup, DefaultChunkSize);
  } catch (std::exception &E) {
    LOG(Sev::Error,
        "Unable to initialise fast sample environment data tree in HDF file.");
    return HDFWriterModule::InitResult::ERROR_IO();
  }
  return FileWriterBase::InitResult::OK();
}

FileWriterBase::InitResult
FastSampleEnvironmentWriter::reopen(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
    Value = NeXusDataset::RawValue(CurrentGroup);
    Timestamp = NeXusDataset::Time(CurrentGroup);
    CueTimestampIndex = NeXusDataset::CueIndex(CurrentGroup);
    CueTimestamp = NeXusDataset::CueTimestampZero(CurrentGroup);
  } catch (std::exception &E) {
    LOG(Sev::Error, "Failed to reopen datasets in HDF file.");
    return HDFWriterModule::InitResult::ERROR_IO();
  }
  return FileWriterBase::InitResult::OK();
}

FileWriterBase::WriteResult
FastSampleEnvironmentWriter::write(const KafkaMessage &Message) {
  auto FbPointer = GetSampleEnvironmentData(Message.data());
  auto TempDataPtr = FbPointer->Values()->data();
  auto TempDataSize = FbPointer->Values()->size();
  if (TempDataSize == 0) {
    return FileWriterBase::WriteResult::OK();
  }
  ArrayAdapter<const std::uint16_t> CArray(TempDataPtr, TempDataSize);
  auto NrOfElements = Value.dataspace().size();
  CueTimestampIndex.appendElement(static_cast<std::uint32_t>(NrOfElements));
  CueTimestamp.appendElement(FbPointer->PacketTimestamp());
  Value.appendData(CArray);
  if (flatbuffers::IsFieldPresent(FbPointer,
                                  SampleEnvironmentData::VT_TIMESTAMPS) and
      FbPointer->Values()->size() == FbPointer->Timestamps()->size()) {
    auto TimestampPtr = FbPointer->Timestamps()->data();
    auto TimestampSize = FbPointer->Timestamps()->size();
    ArrayAdapter<const std::uint64_t> TSArray(TimestampPtr, TimestampSize);
    Timestamp.appendData(TSArray);
  } else {
    std::vector<std::uint64_t> TempTimeStamps(TempDataSize);
    for (int i = 0; i < TempDataSize; i++) {
      TempTimeStamps.at(i) =
          FbPointer->PacketTimestamp() +
          static_cast<std::uint64_t>(i * FbPointer->TimeDelta());
    }
    Timestamp.appendData(TempTimeStamps);
  }
  return FileWriterBase::WriteResult::OK();
}

std::int32_t FastSampleEnvironmentWriter::flush() { return 0; }

std::int32_t FastSampleEnvironmentWriter::close() { return 0; }

void FastSampleEnvironmentWriter::enable_cq(CollectiveQueue *cq,
                                            HDFIDStore *hdf_store,
                                            int mpi_rank) {
  LOG(Sev::Error, "Collective queue not implemented.");
}

} // namespace senv
