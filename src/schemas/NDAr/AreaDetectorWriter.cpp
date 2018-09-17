/** Copyright (C) 2018 European Spallation Source ERIC */

/** \file
 *
 *  \brief Implement classes required to implement the ADC file writing module.
 */

#include "../../helper.h"

#include "AreaDetectorWriter.h"
#include "HDFFile.h"
#include <iostream>
#include <limits>

namespace NDAr {

// Register the timestamp and name extraction class for this module
static FileWriter::FlatbufferReaderRegistry::Registrar<
    AreaDetectorDataGuard>
    RegisterNDArGuard("NDAr");

// Register the file writing part of this module
static FileWriter::HDFWriterModuleRegistry::Registrar<
    AreaDetectorWriter>
    RegisterNDArWriter("NDAr");

bool AreaDetectorDataGuard::verify(
    FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(reinterpret_cast<const std::uint8_t* const>(Message.data()), Message.size());
  return FB_Tables::VerifyNDArrayBuffer(Verifier);
}

uint64_t epicsTimeToNsec(std::uint64_t sec, std::uint64_t nsec) {
  return (std::uint64_t(sec) + 631152000L) * 1000000000L + nsec;
}
  
uint64_t
AreaDetectorDataGuard::timestamp(FlatbufferMessage const &Message) const {
  auto FbPointer = FB_Tables::GetNDArray(Message.data());
  auto epicsTime = FbPointer->epicsTS();
  return epicsTimeToNsec(epicsTime->secPastEpoch(), epicsTime->nsec());
}

std::string AreaDetectorDataGuard::source_name(
    const FileWriter::FlatbufferMessage &Message) const {
  // The flatbuffer schema should probably hold some name mentioning the source of the data
  // Until this is done however, use the one defined here
  // \todo Fix areaDetector name
  return "ADPluginKafka";
}

  /// \brief Parse config JSON structure.
  ///
  /// The default is to use double as the element type and a chunk size of 1 MB.
void AreaDetectorWriter::parse_config(
    std::string const &ConfigurationStream,
    std::string const &ConfigurationModule) {
  // \todo Implement configuration here
}

FileWriterBase::InitResult
AreaDetectorWriter::init_hdf(hdf5::node::Group &HDFGroup,
                                      std::string const &HDFAttributes) {
  const int DefaultChunkSize = 1024;
  try {
    auto &CurrentGroup = HDFGroup;
    NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Create,
                       DefaultChunkSize);
    NeXusDataset::CueIndex(CurrentGroup, NeXusDataset::Mode::Create,
                           DefaultChunkSize);
    NeXusDataset::CueTimestampZero(CurrentGroup, NeXusDataset::Mode::Create,
                                   DefaultChunkSize);
    auto AttributesJson = nlohmann::json::parse(HDFAttributes);
    FileWriter::HDFFile::write_attributes(HDFGroup, &AttributesJson);
  } catch (std::exception &E) {
    LOG(Sev::Error, "Unable to initialise areaDetector data tree in "
                    "HDF file with error message: \"{}\"",
        E.what());
    return HDFWriterModule::InitResult::ERROR_IO();
  }
  return FileWriterBase::InitResult::OK();
}

FileWriterBase::InitResult
AreaDetectorWriter::reopen(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
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

FileWriterBase::WriteResult AreaDetectorWriter::write(
    const FileWriter::FlatbufferMessage &Message) {
//  auto FbPointer = GetSampleEnvironmentData(Message.data());
//  auto TempDataPtr = FbPointer->Values()->data();
//  auto TempDataSize = FbPointer->Values()->size();
//  if (TempDataSize == 0) {
//    LOG(Sev::Warning,
//        "Received a flatbuffer with zero (0) data elements in it.");
//    return FileWriterBase::WriteResult::OK();
//  }
//  ArrayAdapter<const std::uint16_t> CArray(TempDataPtr, TempDataSize);
//  auto CueIndexValue = Value.dataspace().size();
//  CueTimestampIndex.appendElement(static_cast<std::uint32_t>(CueIndexValue));
//  CueTimestamp.appendElement(FbPointer->PacketTimestamp());
//  Value.appendArray(CArray);
//  // Time-stamps are available in the flatbuffer
//  if (flatbuffers::IsFieldPresent(FbPointer,
//                                  SampleEnvironmentData::VT_TIMESTAMPS) and
//      FbPointer->Values()->size() == FbPointer->Timestamps()->size()) {
//    auto TimestampPtr = FbPointer->Timestamps()->data();
//    auto TimestampSize = FbPointer->Timestamps()->size();
//    ArrayAdapter<const std::uint64_t> TSArray(TimestampPtr, TimestampSize);
//    Timestamp.appendArray(TSArray);
//  } else { // If timestamps are not available, generate them
//    std::vector<std::uint64_t> TempTimeStamps(GenerateTimeStamps(
//        FbPointer->PacketTimestamp(), FbPointer->TimeDelta(), TempDataSize));
//    Timestamp.appendArray(TempTimeStamps);
//  }
  return FileWriterBase::WriteResult::OK();
}

std::int32_t AreaDetectorWriter::flush() { return 0; }

std::int32_t AreaDetectorWriter::close() { return 0; }

void AreaDetectorWriter::enable_cq(CollectiveQueue *cq,
                                            HDFIDStore *hdf_store,
                                            int mpi_rank) {
  LOG(Sev::Error, "Collective queue not implemented.");
}

} // namespace NDAr
