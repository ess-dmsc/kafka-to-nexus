#include <HDFWriterModule.h>
#include <gtest/gtest.h>

namespace FileWriter {

using ModuleFactory = HDFWriterModuleRegistry::ModuleFactory;

class WriterRegistrationTest : public ::testing::Test {
public:
  void SetUp() override {
    std::map<std::string, ModuleFactory> &WriterFactories =
        HDFWriterModuleRegistry::getFactories();
    WriterFactories.clear();
  };
};

class DummyWriter : public HDFWriterModule {
public:
  void parse_config(rapidjson::Value const &config_stream,
                    rapidjson::Value const *config_module) override {}
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      rapidjson::Value const *attributes) override {
    return InitResult::OK();
  }
  InitResult reopen(hdf5::node::Group &HDFGrup) override {
    return InitResult::OK();
  }
  WriteResult write(Msg const &msg) override { return WriteResult::OK(); }
  std::int32_t flush() override { return 0; }

  std::int32_t close() override { return 0; }

  void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                 int mpi_rank) override {}
};

TEST_F(WriterRegistrationTest, SimpleRegistration) {
  std::map<std::string, ModuleFactory> &Factories =
      HDFWriterModuleRegistry::getFactories();
  std::string TestKey("temp");
  EXPECT_EQ(Factories.size(), 0);
  { HDFWriterModuleRegistry::Registrar<DummyWriter> RegisterIt(TestKey); }
  EXPECT_EQ(Factories.size(), 1);
  EXPECT_NE(Factories.find(TestKey), Factories.end());
}

TEST_F(WriterRegistrationTest, SameKeyRegistration) {
  std::string TestKey("temp");
  { HDFWriterModuleRegistry::Registrar<DummyWriter> RegisterIt(TestKey); }
  EXPECT_THROW(
      HDFWriterModuleRegistry::Registrar<DummyWriter> RegisterIt(TestKey),
      std::runtime_error);
}

TEST_F(WriterRegistrationTest, KeyToShort) {
  std::string TestKey("tem");
  EXPECT_THROW(
      HDFWriterModuleRegistry::Registrar<DummyWriter> RegisterIt(TestKey),
      std::runtime_error);
}

TEST_F(WriterRegistrationTest, KeyToLong) {
  std::string TestKey("tempp");
  EXPECT_THROW(
      HDFWriterModuleRegistry::Registrar<DummyWriter> RegisterIt(TestKey),
      std::runtime_error);
}

TEST_F(WriterRegistrationTest, StrKeyFound) {
  std::string TestKey("t3mp");
  { HDFWriterModuleRegistry::Registrar<DummyWriter> RegisterIt(TestKey); }
  EXPECT_NE(HDFWriterModuleRegistry::find(TestKey), nullptr);
}

TEST_F(WriterRegistrationTest, StrKeyNotFound) {
  std::string TestKey("t3mp");
  { HDFWriterModuleRegistry::Registrar<DummyWriter> RegisterIt(TestKey); }
  std::string FailKey("trump");
  EXPECT_EQ(HDFWriterModuleRegistry::find(FailKey), nullptr);
}
} // namespace FileWriter
