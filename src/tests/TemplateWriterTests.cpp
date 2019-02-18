#include "schemas/template/TemplateWriter.h"
#include <gtest/gtest.h>

TEST(TemplateTests, ReaderReturnValues) {
  TemplateWriter::ReaderClass SomeReader;
  EXPECT_TRUE(SomeReader.verify(FileWriter::FlatbufferMessage()));
  EXPECT_EQ(SomeReader.source_name(FileWriter::FlatbufferMessage()),
            std::string(""));
  EXPECT_EQ(SomeReader.timestamp(FileWriter::FlatbufferMessage()), 0u);
}

TEST(TemplateTests, WriterReturnValues) {
  TemplateWriter::WriterClass SomeWriter;
  hdf5::node::Group SomeGroup;
  EXPECT_TRUE(SomeWriter.init_hdf(SomeGroup, "{}") ==
              FileWriter::HDFWriterModule_detail::InitResult::OK);
  EXPECT_TRUE(SomeWriter.reopen(SomeGroup) ==
              FileWriter::HDFWriterModule_detail::InitResult::OK);
  EXPECT_NO_THROW(SomeWriter.write(FileWriter::FlatbufferMessage()));
  EXPECT_EQ(SomeWriter.flush(), 0);
  EXPECT_EQ(SomeWriter.close(), 0);
}
