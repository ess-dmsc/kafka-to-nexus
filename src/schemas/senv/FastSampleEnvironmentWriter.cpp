/** Copyright (C) 2018 European Spallation Source ERIC */

/** \file
 *
 *  \brief Implement classes required to implement the ADC file writing module.
 */

#include "../../helper.h"

#include "FastSampleEnvironmentWriter.h"
#include "HDFFile.h"
#include <iostream>
#include <limits>

namespace senv {

// Register the timestamp and name extraction class for this module
static FileWriter::FlatbufferReaderRegistry::Registrar<
    SampleEnvironmentDataGuard>
    RegisterSenvGuard("senv");

// Register the file writing part of this module
static FileWriter::HDFWriterModuleRegistry::Registrar<
    FastSampleEnvironmentWriter>
    RegisterSenvWriter("senv");

bool SampleEnvironmentDataGuard::verify(KafkaMessage const &Message) const {
  auto Verifier =
      flatbuffers::Verifier((uint8_t *)Message.data(), Message.size());
  return VerifySampleEnvironmentDataBuffer(Verifier);
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
    NeXusDataset::RawValue(CurrentGroup, NeXusDataset::Mode::Create,
                           DefaultChunkSize);
    NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Create,
                       DefaultChunkSize);
    NeXusDataset::CueIndex(CurrentGroup, NeXusDataset::Mode::Create,
                           DefaultChunkSize);
    NeXusDataset::CueTimestampZero(CurrentGroup, NeXusDataset::Mode::Create,
                                   DefaultChunkSize);
    if (attributes) {
      FileWriter::HDFFile::write_attributes(HDFGroup, attributes);
    }
  } catch (std::exception &E) {
    LOG(Sev::Error, "Unable to initialise fast sample environment data tree in "
                    "HDF file with error message: \"{}\"",
        E.what());
    return HDFWriterModule::InitResult::ERROR_IO();
  }
  return FileWriterBase::InitResult::OK();
}

FileWriterBase::InitResult
FastSampleEnvironmentWriter::reopen(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
    Value = NeXusDataset::RawValue(CurrentGroup, NeXusDataset::Mode::Open);
    Timestamp = NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestampIndex =
        NeXusDataset::CueIndex(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestamp =
        NeXusDataset::CueTimestampZero(CurrentGroup, NeXusDataset::Mode::Open);
  } catch (std::exception &E) {
    LOG(Sev::Error,
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return HDFWriterModule::InitResult::ERROR_IO();
  }
  return FileWriterBase::InitResult::OK();
}

std::vector<std::uint64_t> GenerateTimeStamps(std::uint64_t OriginTimeStamp,
                                              double TimeDelta,
                                              int NumberOfElements) {
  std::vector<std::uint64_t> ReturnVector(NumberOfElements);
  for (int i = 0; i < NumberOfElements; i++) {
    ReturnVector[i] = OriginTimeStamp + std::llround(i * TimeDelta);
  }
  return ReturnVector;
}

FileWriterBase::WriteResult
FastSampleEnvironmentWriter::write(const KafkaMessage &Message) {
  auto FbPointer = GetSampleEnvironmentData(Message.data());
  auto TempDataPtr = FbPointer->Values()->data();
  auto TempDataSize = FbPointer->Values()->size();
  if (TempDataSize == 0) {
    LOG(Sev::Warning,
        "Received a flatbuffer with zero (0) data elements in it.");
    return FileWriterBase::WriteResult::OK();
  }
  ArrayAdapter<const std::uint16_t> CArray(TempDataPtr, TempDataSize);
  auto CueIndexValue = Value.dataspace().size();
  CueTimestampIndex.appendElement(static_cast<std::uint32_t>(CueIndexValue));
  CueTimestamp.appendElement(FbPointer->PacketTimestamp());
  Value.appendArray(CArray);
  // Time-stamps are available in the flatbuffer
  if (flatbuffers::IsFieldPresent(FbPointer,
                                  SampleEnvironmentData::VT_TIMESTAMPS) and
      FbPointer->Values()->size() == FbPointer->Timestamps()->size()) {
    auto TimestampPtr = FbPointer->Timestamps()->data();
    auto TimestampSize = FbPointer->Timestamps()->size();
    ArrayAdapter<const std::uint64_t> TSArray(TimestampPtr, TimestampSize);
    Timestamp.appendArray(TSArray);
  } else { // If timestamps are not available, generate them
    std::vector<std::uint64_t> TempTimeStamps(GenerateTimeStamps(
        FbPointer->PacketTimestamp(), FbPointer->TimeDelta(), TempDataSize));
    Timestamp.appendArray(TempTimeStamps);
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
