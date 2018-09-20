#include <gtest/gtest.h>
#include "schemas/NDAr/AreaDetectorWriter.h"
#include "schemas/NDAr_NDArray_schema_generated.h"
#include <fstream>
#include <cmath>

class AreaDetectorReader : public ::testing::Test {
public:
  static void SetUpTestCase() {
    std::ifstream InFile(std::string(TEST_DATA_PATH) + "/someNDArray.data",
                         std::ifstream::in | std::ifstream::binary);
    InFile.seekg(0, InFile.end);
    FileSize = InFile.tellg();
    RawData.reset(new char[FileSize]);
    InFile.seekg(0, InFile.beg);
    InFile.read(RawData.get(), FileSize);
  };
  
  static void TearDownTestCase() {};
  
  virtual void SetUp() {
    ASSERT_NE(FileSize, size_t(0));
    Reader = std::make_unique<NDAr::AreaDetectorDataGuard>();
    std::map<std::string, FileWriter::FlatbufferReaderRegistry::ReaderPtr> &Readers =
    FileWriter::FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    FileWriter::FlatbufferReaderRegistry::Registrar<
    NDAr::AreaDetectorDataGuard> RegisterIt("NDAr");
  };
  
  virtual void TearDown() {
  };
  std::unique_ptr<NDAr::AreaDetectorDataGuard> Reader;
  static std::unique_ptr<char[]> RawData;
  static size_t FileSize;
};

std::unique_ptr<char[]> AreaDetectorReader::RawData;
size_t AreaDetectorReader::FileSize = 0;


TEST_F(AreaDetectorReader, ValidateTestOk) {
  FileWriter::FlatbufferMessage Message(RawData.get(), FileSize);
  EXPECT_TRUE(Reader->verify(Message));
}

TEST_F(AreaDetectorReader, ValidateTestFail) {
  flatbuffers::FlatBufferBuilder builder;
  auto epics_ts = FB_Tables::epicsTimeStamp(0, 0);
  auto someDims = builder.CreateVector(std::vector<std::uint64_t>({0, 1, 2, 3,}));
  auto someData = builder.CreateVector(std::vector<std::uint8_t>({0, 1, 2, 3,}));
  auto tmpPkg = FB_Tables::CreateNDArray(builder, 0, 0, &epics_ts, someDims, FB_Tables::DType::Uint8, someData);
  builder.Finish(tmpPkg); //Finish without file identifier will fail verify

  EXPECT_THROW(FileWriter::FlatbufferMessage((char*)builder.GetBufferPointer(), builder.GetSize()), std::runtime_error);
}

//We are currently using a static source name, this should be changed eventually
TEST_F(AreaDetectorReader, SourceNameTest) {
  FileWriter::FlatbufferMessage Message(RawData.get(), FileSize);
  EXPECT_EQ(Reader->source_name(Message), "ADPluginKafka");
}

TEST_F(AreaDetectorReader, TimeStampTest) {
  FileWriter::FlatbufferMessage Message(RawData.get(), FileSize);
  auto tempNDArr = FB_Tables::GetNDArray(RawData.get());
  EXPECT_NE(tempNDArr->epicsTS()->secPastEpoch(), 0);
  EXPECT_NE(tempNDArr->epicsTS()->nsec(), 0);
  std::uint64_t unixEpicsSecDiff = 631152000;
  std::uint64_t secToNsec = 1000000000;
  std::uint64_t tempTimeStamp = (tempNDArr->epicsTS()->secPastEpoch() + unixEpicsSecDiff) * secToNsec;
  tempTimeStamp += tempNDArr->epicsTS()->nsec();
  EXPECT_EQ(Reader->timestamp(Message), tempTimeStamp);
}

class ADWriterStandIn : public NDAr::AreaDetectorWriter {
public:
  using NDAr::AreaDetectorWriter::ChunkSize;
  using NDAr::AreaDetectorWriter::ArrayShape;
  using NDAr::AreaDetectorWriter::Type;
  using NDAr::AreaDetectorWriter::ElementType;
  using NDAr::AreaDetectorWriter::Values;
  using NDAr::AreaDetectorWriter::Timestamp;
  using NDAr::AreaDetectorWriter::CueInterval;
  using NDAr::AreaDetectorWriter::CueTimestamp;
  using NDAr::AreaDetectorWriter::CueTimestampIndex;
};

class NDArrWriter : public ::testing::Test {
public:
  static void SetUpTestCase() {
    std::ifstream InFile(std::string(TEST_DATA_PATH) + "/someNDArray.data",
                         std::ifstream::in | std::ifstream::binary);
    InFile.seekg(0, InFile.end);
    FileSize = InFile.tellg();
    RawData.reset(new char[FileSize]);
    InFile.seekg(0, InFile.beg);
    InFile.read(RawData.get(), FileSize);
  };
  
  static std::unique_ptr<char[]> RawData;
  static size_t FileSize;
  
  virtual void SetUp() override {
    File = hdf5::file::create(TestFileName, hdf5::file::AccessFlags::TRUNCATE);
    RootGroup = File.root();
    UsedGroup = RootGroup.create_group(NXLogGroup);
    std::map<std::string, FileWriter::FlatbufferReaderRegistry::ReaderPtr> &Readers =
    FileWriter::FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    FileWriter::FlatbufferReaderRegistry::Registrar<
    NDAr::AreaDetectorDataGuard> RegisterIt("NDAr");
  };

  void TearDown() override { File.close(); };
  
  std::string TestFileName{"SomeTestFile.hdf5"};
  std::string NXLogGroup{"SomeParentName"};
  hdf5::file::File File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
};
std::unique_ptr<char[]> NDArrWriter::RawData;
size_t NDArrWriter::FileSize = 0;

TEST_F(NDArrWriter, WriterInitTest) {
  {
  ADWriterStandIn Temp;
  Temp.init_hdf(UsedGroup, "{}");
  }
  EXPECT_TRUE(UsedGroup.has_dataset("cue_index"));
  EXPECT_TRUE(UsedGroup.has_dataset("value"));
  EXPECT_TRUE(UsedGroup.has_dataset("time"));
  EXPECT_TRUE(UsedGroup.has_dataset("cue_timestamp_zero"));
}

TEST_F(NDArrWriter, WriterInitFail) {
  {
  ADWriterStandIn Temp;
  Temp.init_hdf(UsedGroup, "{}");
  }
  ADWriterStandIn Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}").is_ERR());
}

TEST_F(NDArrWriter, WriterReOpenFail) {
  ADWriterStandIn Writer;
  EXPECT_TRUE(Writer.reopen(UsedGroup).is_ERR());
}


TEST_F(NDArrWriter, WriterInitInt8) {
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "int8"
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<std::int8_t>(), Writer.Values->datatype());
}

TEST_F(NDArrWriter, WriterInitUInt8) {
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "uint8"
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<std::uint8_t>(), Writer.Values->datatype());
}

TEST_F(NDArrWriter, WriterInitInt16) {
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "int16"
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<std::int16_t>(), Writer.Values->datatype());
}

TEST_F(NDArrWriter, WriterInitUInt16) {
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "uint16"
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<std::uint16_t>(), Writer.Values->datatype());
}

TEST_F(NDArrWriter, WriterInitInt32) {
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "int32"
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<std::int32_t>(), Writer.Values->datatype());
}

TEST_F(NDArrWriter, WriterInitUInt32) {
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "uint32"
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<std::uint32_t>(), Writer.Values->datatype());
}

TEST_F(NDArrWriter, WriterInitInt64) {
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "int64"
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<std::int64_t>(), Writer.Values->datatype());
}

TEST_F(NDArrWriter, WriterInitUInt64) {
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "uint64"
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<std::uint64_t>(), Writer.Values->datatype());
}

TEST_F(NDArrWriter, WriterInitDouble) {
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "float64"
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<std::double_t>(), Writer.Values->datatype());
}

TEST_F(NDArrWriter, WriterInitFloat) {
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "float32"
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<std::float_t>(), Writer.Values->datatype());
}

TEST_F(NDArrWriter, WriterInitChar) {
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "c_string"
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<char>(), Writer.Values->datatype());
}

TEST_F(NDArrWriter, WriterDefaultValuesTest) {
  ADWriterStandIn Temp;
  Temp.init_hdf(UsedGroup, "{}");
  Temp.reopen(UsedGroup);
  EXPECT_EQ(hdf5::datatype::create<double>(), Temp.Values->datatype());
  auto Dataspace = hdf5::dataspace::Simple(Temp.Values->dataspace());
  EXPECT_EQ(Dataspace.maximum_dimensions(), (hdf5::Dimensions{H5S_UNLIMITED, H5S_UNLIMITED, H5S_UNLIMITED}));
  EXPECT_EQ(Dataspace.current_dimensions(), (hdf5::Dimensions{0,1,1}));
  auto CreationProperties = Temp.Values->creation_list();
  auto ChunkDims = CreationProperties.chunk();
  EXPECT_EQ(ChunkDims, (hdf5::Dimensions{64, 1, 1}));
}

TEST_F(NDArrWriter, WriterWriteTest) {
  FileWriter::FlatbufferMessage Message(RawData.get(), FileSize);
  ADWriterStandIn Temp;
  Temp.init_hdf(UsedGroup, "{}");
  Temp.reopen(UsedGroup);
  EXPECT_TRUE(Temp.write(Message).is_OK());
  EXPECT_TRUE(Temp.write(Message).is_OK());
  EXPECT_EQ(2, Temp.Timestamp.dataspace().size());
}

TEST_F(NDArrWriter, WriterCueCounterTest) {
  FileWriter::FlatbufferMessage Message(RawData.get(), FileSize);
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "cue_interval": 3
  })"");
  Writer.parse_config(JsonConfig.dump(),"");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(Writer.write(Message).is_OK());
    if (i < 2) {
      EXPECT_EQ(0, Writer.CueTimestampIndex.dataspace().size());
      EXPECT_EQ(0, Writer.CueTimestamp.dataspace().size());
    } else {
      EXPECT_EQ(1, Writer.CueTimestampIndex.dataspace().size());
      EXPECT_EQ(1, Writer.CueTimestamp.dataspace().size());
    }
  }
}

TEST_F(NDArrWriter, WriterDimensionsTest) {
  FileWriter::FlatbufferMessage Message(RawData.get(), FileSize);
  ADWriterStandIn Writer;
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  EXPECT_TRUE(Writer.write(Message).is_OK());
  auto Dataspace = hdf5::dataspace::Simple(Writer.Values->dataspace());
  EXPECT_EQ((hdf5::Dimensions{1, 10, 12}), Dataspace.current_dimensions());
}

TEST_F(NDArrWriter, WriterTimeStampTest) {
  FileWriter::FlatbufferMessage Message(RawData.get(), FileSize);
  ADWriterStandIn Writer;
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  auto tempNDArr = FB_Tables::GetNDArray(RawData.get());
  auto compTs = NDAr::epicsTimeToNsec(tempNDArr->epicsTS()->secPastEpoch(), tempNDArr->epicsTS()->nsec());
  Writer.write(Message);
  std::uint64_t storedTs;
  Writer.Timestamp.read(storedTs);
  EXPECT_EQ(compTs, storedTs);
}

TEST_F(NDArrWriter, ConfigTypeTest) {
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "int32"
  })"");
  ADWriterStandIn Writer;
  EXPECT_EQ(Writer.ElementType, ADWriterStandIn::Type::float64);
  Writer.parse_config(JsonConfig.dump(), "");
  EXPECT_EQ(Writer.ElementType, ADWriterStandIn::Type::int32);
}

TEST_F(NDArrWriter, ConfigTypeFailureTest) {
  auto JsonConfig = nlohmann::json::parse(R""({
    "type": "int33"
  })"");
  ADWriterStandIn Writer;
  EXPECT_EQ(Writer.ElementType, ADWriterStandIn::Type::float64);
  Writer.parse_config(JsonConfig.dump(), "");
  EXPECT_EQ(Writer.ElementType, ADWriterStandIn::Type::float64);
}

TEST_F(NDArrWriter, ConfigCueIntervalTest) {
  auto JsonConfig = nlohmann::json::parse(R""({
    "cue_interval": 42
  })"");
  ADWriterStandIn Writer;
  EXPECT_EQ(Writer.CueInterval, 1000);
  Writer.parse_config(JsonConfig.dump(), "");
  EXPECT_EQ(Writer.CueInterval, 42);
}

TEST_F(NDArrWriter, ConfigCueIntervalFailureTest) {
  auto JsonConfig = nlohmann::json::parse(R""({
    "cue_interval": "some_text"
  })"");
  ADWriterStandIn Writer;
  EXPECT_EQ(Writer.CueInterval, 1000);
  Writer.parse_config(JsonConfig.dump(), "");
  EXPECT_EQ(Writer.CueInterval, 1000);
}

TEST_F(NDArrWriter, ConfigArraySizeTest) {
  auto JsonConfig = nlohmann::json::parse(R""({
    "array_size": [5,5,5]
  })"");
  ADWriterStandIn Writer;
  EXPECT_EQ(Writer.ArrayShape, (hdf5::Dimensions{1,1}));
  Writer.parse_config(JsonConfig.dump(), "");
  EXPECT_EQ(Writer.ArrayShape, (hdf5::Dimensions{5,5,5}));
}

TEST_F(NDArrWriter, ConfigArraySizeFailureTest) {
  auto JsonConfig = nlohmann::json::parse(R""({
    "array_size": "hello"
  })"");
  ADWriterStandIn Writer;
  EXPECT_EQ(Writer.ArrayShape, (hdf5::Dimensions{1,1}));
  Writer.parse_config(JsonConfig.dump(), "");
  EXPECT_EQ(Writer.ArrayShape, (hdf5::Dimensions{1,1}));
}

TEST_F(NDArrWriter, ConfigChunkSizeTestAlt) {
  auto JsonConfig = nlohmann::json::parse(R""({
    "chunk_size": 1024
  })"");
  ADWriterStandIn Writer;
  EXPECT_EQ(Writer.ChunkSize, (hdf5::Dimensions{64}));
  Writer.parse_config(JsonConfig.dump(), "");
  EXPECT_EQ(Writer.ChunkSize, (hdf5::Dimensions{1024}));
}

TEST_F(NDArrWriter, ConfigChunkSizeTest) {
  auto JsonConfig = nlohmann::json::parse(R""({
    "chunk_size": [5,5,5,5]
  })"");
  ADWriterStandIn Writer;
  EXPECT_EQ(Writer.ChunkSize, (hdf5::Dimensions{64}));
  Writer.parse_config(JsonConfig.dump(), "");
  EXPECT_EQ(Writer.ChunkSize, (hdf5::Dimensions{5,5,5,5}));
}

TEST_F(NDArrWriter, ConfigChunkSizeFailureTest) {
  auto JsonConfig = nlohmann::json::parse(R""({
    "chunk_size": "hello"
  })"");
  ADWriterStandIn Writer;
  EXPECT_EQ(Writer.ChunkSize, (hdf5::Dimensions{64}));
  Writer.parse_config(JsonConfig.dump(), "");
  EXPECT_EQ(Writer.ChunkSize, (hdf5::Dimensions{64}));
}

// Note, you must feed it 1000 elements in total
void GenerateFlatbuffer(flatbuffers::FlatBufferBuilder &Builder, std::uint8_t *DataPtr, size_t DataBytes, FB_Tables::DType Type) {
  hdf5::Dimensions storeDims = {10, 10, 10};
  std::vector<std::uint64_t> fbTypeDims(storeDims.begin(), storeDims.end());
  auto fbDims = Builder.CreateVector<std::uint64_t>(fbTypeDims);
  std::uint8_t *TempPtr;
  auto payload = Builder.CreateUninitializedVector(DataBytes, 1, &TempPtr);
  std::memcpy(TempPtr, DataPtr, DataBytes);
  FB_Tables::NDArrayBuilder arrayBuilder(Builder);
  arrayBuilder.add_dims(fbDims);
  arrayBuilder.add_pData(payload);
  auto Seconds = 1;
  auto NanoSeconds = 2;
  auto IdNr = 42;
  auto Timestamp = 3.14;
  auto epics_ts = FB_Tables::epicsTimeStamp(Seconds, NanoSeconds);
  arrayBuilder.add_epicsTS(&epics_ts);
  arrayBuilder.add_id(IdNr);
  arrayBuilder.add_timeStamp(Timestamp);
  arrayBuilder.add_dataType(Type);
  auto kf_pkg = arrayBuilder.Finish();
  // Write data to buffer
  Builder.Finish(kf_pkg, FB_Tables::NDArrayIdentifier());
}

template <class Type>
bool WriteTest(hdf5::node::Group &UsedGroup, FB_Tables::DType FBType) {
  std::vector<Type> testData;
  for (int j = 0; j < 10*10*10; j++) {
    testData.push_back(j);
  }
  flatbuffers::FlatBufferBuilder builder;
  GenerateFlatbuffer(builder, (std::uint8_t*)&testData[0], 1000 * (sizeof(testData[0])), FBType);
  
  FileWriter::FlatbufferMessage Message((char*) builder.GetBufferPointer(), builder.GetSize());
  ADWriterStandIn Writer;
  auto JsonConfig = nlohmann::json::parse(R""({
    "array_size": [10,10,10]
  })"");
  Writer.parse_config(JsonConfig.dump(), "");
  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);
  if (Writer.write(Message).is_ERR()) {
    return false;
  }
  std::vector<Type> dataFromFile(testData.size());
  hdf5::Dimensions CDims = hdf5::dataspace::Simple(Writer.Values->dataspace()).current_dimensions();
  hdf5::Dimensions ExpectedDims{{1, 10,10,10}};
  EXPECT_EQ(CDims, ExpectedDims);
  Writer.Values->read(dataFromFile);
  EXPECT_EQ(dataFromFile, testData);
  return true;
}

TEST_F(NDArrWriter, WriterInt8Test) {
  EXPECT_TRUE(WriteTest<std::int8_t>(UsedGroup, FB_Tables::DType::Int8));
}

TEST_F(NDArrWriter, WriterUInt8Test) {
  EXPECT_TRUE(WriteTest<std::uint8_t>(UsedGroup, FB_Tables::DType::Uint8));
}

TEST_F(NDArrWriter, WriterInt16Test) {
  EXPECT_TRUE(WriteTest<std::int16_t>(UsedGroup, FB_Tables::DType::Int16));
}

TEST_F(NDArrWriter, WriterUInt16Test) {
  EXPECT_TRUE(WriteTest<std::uint16_t>(UsedGroup, FB_Tables::DType::Uint16));
}

TEST_F(NDArrWriter, WriterInt32Test) {
  EXPECT_TRUE(WriteTest<std::int32_t>(UsedGroup, FB_Tables::DType::Int32));
}

TEST_F(NDArrWriter, WriterUInt32Test) {
  EXPECT_TRUE(WriteTest<std::uint32_t>(UsedGroup, FB_Tables::DType::Uint32));
}

TEST_F(NDArrWriter, WriterFloatTest) {
  EXPECT_TRUE(WriteTest<float>(UsedGroup, FB_Tables::DType::Float32));
}

TEST_F(NDArrWriter, WriterDoubleTest) {
  EXPECT_TRUE(WriteTest<double>(UsedGroup, FB_Tables::DType::Float64));
}

TEST_F(NDArrWriter, WriterCharTest) {
  EXPECT_TRUE(WriteTest<char>(UsedGroup, FB_Tables::DType::c_string));
}

TEST_F(NDArrWriter, WriterWrongFBTypeTest) {
  EXPECT_FALSE(WriteTest<char>(UsedGroup, FB_Tables::DType(9999)));
}
