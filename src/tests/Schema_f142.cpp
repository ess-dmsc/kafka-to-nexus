// This filename is chosen such that one can search easily after its name even
// in case-sensitive search.

#include "../schemas/f142/f142_rw.h"
#include <gtest/gtest.h>

TEST(Schema_f142, basic) {
  ASSERT_FALSE(
      FileWriter::Schemas::f142::Bla<FileWriter::Schemas::f142::Value::UByte>()
          .IsArray);
  ASSERT_TRUE(FileWriter::Schemas::f142::Bla<
                  FileWriter::Schemas::f142::Value::ArrayUByte>()
                  .IsArray);
}
