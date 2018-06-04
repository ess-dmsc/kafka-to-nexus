#include "schemas/template/TemplateWriter.h"
#include <gtest/gtest.h>

TEST(TemplateTests, ReaderReturnValues) {
  TemplateWriter::ReaderClass SomeReader;
  EXPECT_TRUE(SomeReader.verify(FileWriter::Msg()));
  EXPECT_EQ(SomeReader.source_name(FileWriter::Msg()), std::string(""));
  EXPECT_EQ(SomeReader.timestamp(FileWriter::Msg()), 0u);
}

TEST(TemplateTests, WriterReturnValues) {
  TemplateWriter::WriterClass SomeWriter;
  hdf5::node::Group SomeGroup;
  EXPECT_TRUE(SomeWriter.init_hdf(SomeGroup, "{}").is_OK());
  EXPECT_TRUE(SomeWriter.reopen(SomeGroup).is_OK());
  EXPECT_TRUE(SomeWriter.write(FileWriter::Msg()).is_OK());
  EXPECT_EQ(SomeWriter.flush(), 0);
  EXPECT_EQ(SomeWriter.close(), 0);
}
